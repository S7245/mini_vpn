use crate::shared::TargetAddr;
use bytes::{BufMut, Bytes, BytesMut};
use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroU64;
use thiserror::Error;

const MAGIC: [u8; 4] = *b"MVPN";
const DATA_FIXED_BODY_BYTES: usize = 8 + 8 + 1 + 8;
const ATTACH_FIXED_BODY_BYTES: usize = 8 + 16 + 16 + 2 + 2 + 8 + 8 + 32;
const ATTACH_ACCEPTED_FIXED_BODY_BYTES: usize = 8 + 16 + 16 + 2 + 8;
const ATTACH_GENERATION_STATUS_FIXED_BODY_BYTES: usize = 8 + 16 + 8 + 16;
const STANDBY_REGISTER_FIXED_BODY_BYTES: usize = 8 + 16 + 16 + 2 + 8 + 32;
const STANDBY_ACCEPTED_FIXED_BODY_BYTES: usize = 8 + 16 + 16 + 2 + 8;
const LEG_PROBE_FIXED_BODY_BYTES: usize = 8 + 16 + 16 + 8;
const LEG_PROBE_ACK_FIXED_BODY_BYTES: usize = 8 + 16 + 16 + 8;
const SWITCH_HINT_FIXED_BODY_BYTES: usize = 8 + 16 + 16 + 8 + 8 + 1 + 8 + 2;
const OPEN_RESULT_FIXED_BODY_BYTES: usize = 8 + 8 + 2;
const ACK_FIXED_BODY_BYTES: usize = 8 + 8 + 1 + 8 + 1;
const CLOSE_FIXED_BODY_BYTES: usize = 8 + 8 + 1 + 8;
const RESET_FIXED_BODY_BYTES: usize = 8 + 8 + 2;
const OPEN_PREFIX_BODY_BYTES: usize = 8 + 8;
const MAX_DOMAIN_BYTES: usize = 253;
const MIN_OPEN_BODY_BYTES: usize = OPEN_PREFIX_BODY_BYTES + 1 + 2 + 1 + 1;
const MAX_OPEN_BODY_BYTES: usize = OPEN_PREFIX_BODY_BYTES + 1 + 2 + 1 + MAX_DOMAIN_BYTES;
const RECORD_ATTACH: u8 = 0x01;
const RECORD_OPEN: u8 = 0x02;
const RECORD_DATA: u8 = 0x03;
const RECORD_ACK: u8 = 0x04;
const RECORD_CLOSE: u8 = 0x05;
const RECORD_RESET: u8 = 0x06;
const RECORD_ATTACH_ACCEPTED: u8 = 0x07;
const RECORD_OPEN_RESULT: u8 = 0x08;
const RECORD_ATTACH_GENERATION_STATUS: u8 = 0x09;
const RECORD_STANDBY_REGISTER: u8 = 0x0a;
const RECORD_STANDBY_ACCEPTED: u8 = 0x0b;
const RECORD_LEG_PROBE: u8 = 0x0c;
const RECORD_LEG_PROBE_ACK: u8 = 0x0d;
const RECORD_SWITCH_HINT: u8 = 0x0e;
const TARGET_DOMAIN: u8 = 0x00;
const TARGET_IPV4: u8 = 0x01;
const TARGET_IPV6: u8 = 0x02;

pub const FRAME_HEADER_BYTES: usize = 12;
/// Version of the fixed frame envelope and record parser.
pub const FRAME_PROTOCOL_VERSION: u16 = 1;
/// Current negotiated resumable-session semantics carried by ATTACH.
pub const SESSION_PROTOCOL_VERSION: u16 = 1;
/// Named feature gate for the five Knife16 standby leg-control records.
pub const STANDBY_CONTROL_V1: FeatureSet = FeatureSet::new(0x10);
/// Hard wire-defense ceiling for one DATA record. Replay ownership limits are
/// injected policy and are deliberately separate from this framing limit.
pub const MAX_DATA_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId([u8; 16]);

impl SessionId {
    pub fn new(value: [u8; 16]) -> Result<Self, ProtocolError> {
        if value == [0; 16] {
            return Err(ProtocolError::ZeroSessionId);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionId([REDACTED])")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttachNonce([u8; 16]);

impl AttachNonce {
    pub fn new(value: [u8; 16]) -> Result<Self, ProtocolError> {
        if value == [0; 16] {
            return Err(ProtocolError::ZeroAttachNonce);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for AttachNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AttachNonce([REDACTED])")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttachProof([u8; 32]);

impl AttachProof {
    pub fn new(value: [u8; 32]) -> Result<Self, ProtocolError> {
        if value == [0; 32] {
            return Err(ProtocolError::ZeroAttachProof);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for AttachProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AttachProof([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionFlowId(NonZeroU64);

impl SessionFlowId {
    pub fn new(value: u64) -> Result<Self, ProtocolError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProtocolError::ZeroFlowId)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LegGeneration(NonZeroU64);

impl LegGeneration {
    pub fn new(value: u64) -> Result<Self, ProtocolError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProtocolError::ZeroLegGeneration)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteOffset(u64);

impl ByteOffset {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_advance(self, bytes: usize) -> Result<Self, ProtocolError> {
        let bytes = u64::try_from(bytes).map_err(|_| ProtocolError::OffsetOverflow)?;
        self.0
            .checked_add(bytes)
            .map(Self)
            .ok_or(ProtocolError::OffsetOverflow)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Direction {
    ClientToTarget,
    TargetToClient,
}

impl Direction {
    fn encode(self) -> u8 {
        match self {
            Self::ClientToTarget => 0,
            Self::TargetToClient => 1,
        }
    }

    fn decode(value: u8) -> Result<Self, ProtocolError> {
        match value {
            0 => Ok(Self::ClientToTarget),
            1 => Ok(Self::TargetToClient),
            _ => Err(ProtocolError::InvalidDirection(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct FeatureSet(u64);

impl FeatureSet {
    /// Negotiates the Knife16 registered-standby control-plane records.
    pub const STANDBY_CONTROL_V1: Self = STANDBY_CONTROL_V1;

    pub const fn new(bits: u64) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StandbyNonce([u8; 16]);

impl StandbyNonce {
    pub fn new(value: [u8; 16]) -> Result<Self, ProtocolError> {
        if value == [0; 16] {
            return Err(ProtocolError::ZeroStandbyNonce);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for StandbyNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StandbyNonce([REDACTED])")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LegEpochNonce([u8; 16]);

impl LegEpochNonce {
    pub fn new(value: [u8; 16]) -> Result<Self, ProtocolError> {
        if value == [0; 16] {
            return Err(ProtocolError::ZeroLegEpochNonce);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl From<AttachNonce> for LegEpochNonce {
    fn from(value: AttachNonce) -> Self {
        Self(value.0)
    }
}

impl From<StandbyNonce> for LegEpochNonce {
    fn from(value: StandbyNonce) -> Self {
        Self(value.0)
    }
}

impl fmt::Debug for LegEpochNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegEpochNonce([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProbeSequence(NonZeroU64);

impl ProbeSequence {
    pub fn new(value: u64) -> Result<Self, ProtocolError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProtocolError::ZeroProbeSequence)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    pub fn checked_next(self) -> Result<Self, ProtocolError> {
        self.get()
            .checked_add(1)
            .ok_or(ProtocolError::ProbeSequenceOverflow)
            .and_then(Self::new)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HintSequence(NonZeroU64);

impl HintSequence {
    pub fn new(value: u64) -> Result<Self, ProtocolError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProtocolError::ZeroHintSequence)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    pub fn checked_next(self) -> Result<Self, ProtocolError> {
        self.get()
            .checked_add(1)
            .ok_or(ProtocolError::HintSequenceOverflow)
            .and_then(Self::new)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StandbyProof([u8; 32]);

impl StandbyProof {
    pub fn new(value: [u8; 32]) -> Result<Self, ProtocolError> {
        if value == [0; 32] {
            return Err(ProtocolError::ZeroStandbyProof);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for StandbyProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StandbyProof([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum OpenResultCode {
    Opened = 0,
    TargetRefused = 1,
    TargetUnreachable = 2,
    TimedOut = 3,
    ResourceExhausted = 4,
    PolicyDenied = 5,
    Internal = 6,
}

impl OpenResultCode {
    fn decode(value: u16) -> Result<Self, ProtocolError> {
        match value {
            0 => Ok(Self::Opened),
            1 => Ok(Self::TargetRefused),
            2 => Ok(Self::TargetUnreachable),
            3 => Ok(Self::TimedOut),
            4 => Ok(Self::ResourceExhausted),
            5 => Ok(Self::PolicyDenied),
            6 => Ok(Self::Internal),
            other => Err(ProtocolError::InvalidOpenResult(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ResetReason {
    Unspecified = 0,
    ProtocolViolation = 1,
    LocalAbandon = 2,
    TargetFailure = 3,
    ResumeExpired = 4,
    ResourceExhausted = 5,
}

impl ResetReason {
    fn decode(value: u16) -> Result<Self, ProtocolError> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::ProtocolViolation),
            2 => Ok(Self::LocalAbandon),
            3 => Ok(Self::TargetFailure),
            4 => Ok(Self::ResumeExpired),
            5 => Ok(Self::ResourceExhausted),
            other => Err(ProtocolError::InvalidResetReason(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum SwitchHintCause {
    ReverseApplicationAckStall = 1,
}

impl SwitchHintCause {
    fn decode(value: u16) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::ReverseApplicationAckStall),
            other => Err(ProtocolError::InvalidSwitchHintCause(other)),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Record {
    Attach {
        session_id: SessionId,
        nonce: AttachNonce,
        min_version: u16,
        max_version: u16,
        offered_features: FeatureSet,
        required_features: FeatureSet,
        proof: AttachProof,
    },
    AttachAccepted {
        session_id: SessionId,
        nonce: AttachNonce,
        selected_version: u16,
        features: FeatureSet,
    },
    AttachGenerationStatus {
        session_id: SessionId,
        requested_generation: LegGeneration,
        nonce: AttachNonce,
    },
    Open {
        flow_id: SessionFlowId,
        target: TargetAddr,
    },
    Data {
        flow_id: SessionFlowId,
        direction: Direction,
        offset: ByteOffset,
        payload: Bytes,
    },
    OpenResult {
        flow_id: SessionFlowId,
        result: OpenResultCode,
    },
    Ack {
        flow_id: SessionFlowId,
        direction: Direction,
        next_accepted: ByteOffset,
        final_accepted: bool,
    },
    Close {
        flow_id: SessionFlowId,
        direction: Direction,
        final_offset: ByteOffset,
    },
    Reset {
        flow_id: SessionFlowId,
        reason: ResetReason,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub enum LegControlRecord {
    StandbyRegister {
        session_id: SessionId,
        standby_nonce: StandbyNonce,
        selected_version: u16,
        features: FeatureSet,
        proof: StandbyProof,
    },
    StandbyAccepted {
        session_id: SessionId,
        standby_nonce: StandbyNonce,
        selected_version: u16,
        features: FeatureSet,
    },
    LegProbe {
        session_id: SessionId,
        leg_epoch_nonce: LegEpochNonce,
        probe_sequence: ProbeSequence,
    },
    LegProbeAck {
        session_id: SessionId,
        leg_epoch_nonce: LegEpochNonce,
        probe_sequence: ProbeSequence,
    },
    SwitchHint {
        session_id: SessionId,
        standby_nonce: StandbyNonce,
        hint_sequence: HintSequence,
        flow_id: SessionFlowId,
        direction: Direction,
        oldest_unacknowledged: ByteOffset,
        cause: SwitchHintCause,
    },
}

impl fmt::Debug for LegControlRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StandbyRegister {
                selected_version,
                features,
                ..
            } => formatter
                .debug_struct("StandbyRegister")
                .field("authentication", &"[REDACTED]")
                .field("selected_version", selected_version)
                .field("features", features)
                .finish(),
            Self::LegProbe { probe_sequence, .. } => formatter
                .debug_struct("LegProbe")
                .field("correlation", &"[REDACTED]")
                .field("probe_sequence", probe_sequence)
                .finish(),
            Self::LegProbeAck { probe_sequence, .. } => formatter
                .debug_struct("LegProbeAck")
                .field("correlation", &"[REDACTED]")
                .field("probe_sequence", probe_sequence)
                .finish(),
            Self::StandbyAccepted {
                selected_version,
                features,
                ..
            } => formatter
                .debug_struct("StandbyAccepted")
                .field("correlation", &"[REDACTED]")
                .field("selected_version", selected_version)
                .field("features", features)
                .finish(),
            Self::SwitchHint {
                hint_sequence,
                flow_id,
                direction,
                oldest_unacknowledged,
                cause,
                ..
            } => formatter
                .debug_struct("SwitchHint")
                .field("correlation", &"[REDACTED]")
                .field("hint_sequence", hint_sequence)
                .field("flow_id", flow_id)
                .field("direction", direction)
                .field("oldest_unacknowledged", oldest_unacknowledged)
                .field("cause", cause)
                .finish(),
        }
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Attach {
                min_version,
                max_version,
                offered_features,
                required_features,
                ..
            } => formatter
                .debug_struct("Attach")
                .field("authentication", &"[REDACTED]")
                .field("min_version", min_version)
                .field("max_version", max_version)
                .field("offered_features", offered_features)
                .field("required_features", required_features)
                .finish(),
            Self::AttachAccepted {
                selected_version,
                features,
                ..
            } => formatter
                .debug_struct("AttachAccepted")
                .field("correlation", &"[REDACTED]")
                .field("selected_version", selected_version)
                .field("features", features)
                .finish(),
            Self::AttachGenerationStatus {
                requested_generation,
                ..
            } => formatter
                .debug_struct("AttachGenerationStatus")
                .field("authentication", &"[REDACTED]")
                .field("requested_generation", requested_generation)
                .finish(),
            Self::Open { flow_id, .. } => formatter
                .debug_struct("Open")
                .field("flow_id", flow_id)
                .field("target", &"[REDACTED]")
                .finish(),
            Self::Data {
                flow_id,
                direction,
                offset,
                payload,
            } => formatter
                .debug_struct("Data")
                .field("flow_id", flow_id)
                .field("direction", direction)
                .field("offset", offset)
                .field("payload_len", &payload.len())
                .finish(),
            Self::OpenResult { flow_id, result } => formatter
                .debug_struct("OpenResult")
                .field("flow_id", flow_id)
                .field("result", result)
                .finish(),
            Self::Ack {
                flow_id,
                direction,
                next_accepted,
                final_accepted,
            } => formatter
                .debug_struct("Ack")
                .field("flow_id", flow_id)
                .field("direction", direction)
                .field("next_accepted", next_accepted)
                .field("final_accepted", final_accepted)
                .finish(),
            Self::Close {
                flow_id,
                direction,
                final_offset,
            } => formatter
                .debug_struct("Close")
                .field("flow_id", flow_id)
                .field("direction", direction)
                .field("final_offset", final_offset)
                .finish(),
            Self::Reset { flow_id, reason } => formatter
                .debug_struct("Reset")
                .field("flow_id", flow_id)
                .field("reason", reason)
                .finish(),
        }
    }
}

/// A header whose kind and declared body size have passed the same checks used
/// by the full decoder. Streaming adapters must obtain this value before they
/// reserve or read a frame body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedFrameHeader {
    record_type: u8,
    body_len: usize,
}

impl ValidatedFrameHeader {
    pub fn decode(encoded: &[u8]) -> Result<Self, ProtocolError> {
        Self::decode_with_features(encoded, FeatureSet::default())
    }

    pub fn decode_with_features(
        encoded: &[u8],
        negotiated_features: FeatureSet,
    ) -> Result<Self, ProtocolError> {
        if encoded.len() < FRAME_HEADER_BYTES {
            return Err(ProtocolError::Truncated {
                needed: FRAME_HEADER_BYTES,
                available: encoded.len(),
            });
        }
        if encoded[..4] != MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }
        let version = u16::from_be_bytes([encoded[4], encoded[5]]);
        if version != FRAME_PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(version));
        }
        let record_type = encoded[6];
        let flags = encoded[7];
        if flags != 0 {
            return Err(ProtocolError::NonZeroFlags(flags));
        }
        let body_len =
            u32::from_be_bytes([encoded[8], encoded[9], encoded[10], encoded[11]]) as usize;
        validate_declared_body_len(record_type, body_len)?;
        require_standby_control_feature(record_type, negotiated_features)?;
        Ok(Self {
            record_type,
            body_len,
        })
    }

    pub const fn body_len(self) -> usize {
        self.body_len
    }

    pub fn total_len(self) -> Result<usize, ProtocolError> {
        FRAME_HEADER_BYTES
            .checked_add(self.body_len)
            .ok_or(ProtocolError::FrameTooLarge)
    }

    /// Decode an already-admitted body without concatenating it back with its
    /// header. The body is compacted once so a tiny DATA slice cannot retain
    /// an uncharged, attacker-sized backing allocation.
    pub fn decode_body(self, body: Bytes) -> Result<Frame, ProtocolError> {
        if body.len() < self.body_len {
            return Err(ProtocolError::Truncated {
                needed: self.body_len,
                available: body.len(),
            });
        }
        if body.len() != self.body_len {
            return Err(ProtocolError::TrailingBytes {
                expected: self.body_len,
                actual: body.len(),
            });
        }
        self.decode_compact_body(Bytes::copy_from_slice(&body))
    }

    pub fn decode_classified_body(self, body: Bytes) -> Result<DecodedFrame, ProtocolError> {
        if body.len() < self.body_len {
            return Err(ProtocolError::Truncated {
                needed: self.body_len,
                available: body.len(),
            });
        }
        if body.len() != self.body_len {
            return Err(ProtocolError::TrailingBytes {
                expected: self.body_len,
                actual: body.len(),
            });
        }
        self.decode_classified_compact_body(Bytes::copy_from_slice(&body))
    }

    fn decode_classified_compact_body(self, body: Bytes) -> Result<DecodedFrame, ProtocolError> {
        match self.record_type {
            RECORD_STANDBY_REGISTER => decode_standby_register(body).map(DecodedFrame::LegControl),
            RECORD_STANDBY_ACCEPTED => decode_standby_accepted(body).map(DecodedFrame::LegControl),
            RECORD_LEG_PROBE => decode_leg_probe(body).map(DecodedFrame::LegControl),
            RECORD_LEG_PROBE_ACK => decode_leg_probe_ack(body).map(DecodedFrame::LegControl),
            RECORD_SWITCH_HINT => decode_switch_hint(body).map(DecodedFrame::LegControl),
            _ => self.decode_compact_body(body).map(DecodedFrame::Session),
        }
    }

    fn decode_compact_body(self, body: Bytes) -> Result<Frame, ProtocolError> {
        match self.record_type {
            RECORD_ATTACH => decode_attach(body),
            RECORD_ATTACH_ACCEPTED => decode_attach_accepted(body),
            RECORD_ATTACH_GENERATION_STATUS => decode_attach_generation_status(body),
            RECORD_OPEN => decode_open(body),
            RECORD_OPEN_RESULT => decode_open_result(body),
            RECORD_DATA => decode_data(body),
            RECORD_ACK => decode_ack(body),
            RECORD_CLOSE => decode_close(body),
            RECORD_RESET => decode_reset(body),
            RECORD_STANDBY_REGISTER
            | RECORD_STANDBY_ACCEPTED
            | RECORD_LEG_PROBE
            | RECORD_LEG_PROBE_ACK
            | RECORD_SWITCH_HINT => Err(ProtocolError::LegControlRequiresClassification),
            other => Err(ProtocolError::UnknownRecordType(other)),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LegControlFrame {
    leg_generation: LegGeneration,
    record: LegControlRecord,
}

impl fmt::Debug for LegControlFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegControlFrame")
            .field("leg_generation", &self.leg_generation)
            .field("record", &self.record)
            .finish()
    }
}

impl LegControlFrame {
    pub fn try_new(
        leg_generation: LegGeneration,
        record: LegControlRecord,
        negotiated_features: FeatureSet,
    ) -> Result<Self, ProtocolError> {
        require_negotiated_standby_control(negotiated_features)?;
        validate_leg_control_record(&record)?;
        Ok(Self {
            leg_generation,
            record,
        })
    }

    fn new(leg_generation: LegGeneration, record: LegControlRecord) -> Self {
        Self {
            leg_generation,
            record,
        }
    }

    pub fn leg_generation(&self) -> LegGeneration {
        self.leg_generation
    }

    pub fn record(&self) -> &LegControlRecord {
        &self.record
    }

    pub fn encode(&self, negotiated_features: FeatureSet) -> Result<Bytes, ProtocolError> {
        require_negotiated_standby_control(negotiated_features)?;
        validate_leg_control_record(&self.record)?;
        match &self.record {
            LegControlRecord::StandbyRegister {
                session_id,
                standby_nonce,
                selected_version,
                features,
                proof,
            } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + STANDBY_REGISTER_FIXED_BODY_BYTES);
                encode_header(
                    &mut encoded,
                    RECORD_STANDBY_REGISTER,
                    STANDBY_REGISTER_FIXED_BODY_BYTES,
                )?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.extend_from_slice(standby_nonce.as_bytes());
                encoded.put_u16(*selected_version);
                encoded.put_u64(features.bits());
                encoded.extend_from_slice(proof.as_bytes());
                Ok(encoded.freeze())
            }
            LegControlRecord::StandbyAccepted {
                session_id,
                standby_nonce,
                selected_version,
                features,
            } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + STANDBY_ACCEPTED_FIXED_BODY_BYTES);
                encode_header(
                    &mut encoded,
                    RECORD_STANDBY_ACCEPTED,
                    STANDBY_ACCEPTED_FIXED_BODY_BYTES,
                )?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.extend_from_slice(standby_nonce.as_bytes());
                encoded.put_u16(*selected_version);
                encoded.put_u64(features.bits());
                Ok(encoded.freeze())
            }
            LegControlRecord::LegProbe {
                session_id,
                leg_epoch_nonce,
                probe_sequence,
            } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + LEG_PROBE_FIXED_BODY_BYTES);
                encode_header(&mut encoded, RECORD_LEG_PROBE, LEG_PROBE_FIXED_BODY_BYTES)?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.extend_from_slice(leg_epoch_nonce.as_bytes());
                encoded.put_u64(probe_sequence.get());
                Ok(encoded.freeze())
            }
            LegControlRecord::LegProbeAck {
                session_id,
                leg_epoch_nonce,
                probe_sequence,
            } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + LEG_PROBE_ACK_FIXED_BODY_BYTES);
                encode_header(
                    &mut encoded,
                    RECORD_LEG_PROBE_ACK,
                    LEG_PROBE_ACK_FIXED_BODY_BYTES,
                )?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.extend_from_slice(leg_epoch_nonce.as_bytes());
                encoded.put_u64(probe_sequence.get());
                Ok(encoded.freeze())
            }
            LegControlRecord::SwitchHint {
                session_id,
                standby_nonce,
                hint_sequence,
                flow_id,
                direction,
                oldest_unacknowledged,
                cause,
            } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + SWITCH_HINT_FIXED_BODY_BYTES);
                encode_header(
                    &mut encoded,
                    RECORD_SWITCH_HINT,
                    SWITCH_HINT_FIXED_BODY_BYTES,
                )?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.extend_from_slice(standby_nonce.as_bytes());
                encoded.put_u64(hint_sequence.get());
                encoded.put_u64(flow_id.get());
                encoded.put_u8(direction.encode());
                encoded.put_u64(oldest_unacknowledged.get());
                encoded.put_u16(*cause as u16);
                Ok(encoded.freeze())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedFrame {
    Session(Frame),
    LegControl(LegControlFrame),
}

impl DecodedFrame {
    pub fn decode_exact(
        encoded: &[u8],
        negotiated_features: FeatureSet,
    ) -> Result<Self, ProtocolError> {
        let header = ValidatedFrameHeader::decode_with_features(encoded, negotiated_features)?;
        let total_len = header.total_len()?;
        validate_exact_frame_len(encoded.len(), total_len)?;
        header
            .decode_classified_compact_body(Bytes::copy_from_slice(&encoded[FRAME_HEADER_BYTES..]))
    }

    pub fn decode_owned_exact(
        encoded: Bytes,
        negotiated_features: FeatureSet,
    ) -> Result<Self, ProtocolError> {
        let header = ValidatedFrameHeader::decode_with_features(&encoded, negotiated_features)?;
        let total_len = header.total_len()?;
        validate_exact_frame_len(encoded.len(), total_len)?;
        header
            .decode_classified_compact_body(Bytes::copy_from_slice(&encoded[FRAME_HEADER_BYTES..]))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Frame {
    leg_generation: LegGeneration,
    record: Record,
}

impl fmt::Debug for Frame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Frame")
            .field("leg_generation", &self.leg_generation)
            .field("record", &self.record)
            .finish()
    }
}

impl Frame {
    /// Constructs a frame only after its record satisfies the same semantic
    /// constraints enforced by the wire encoder and decoder.
    ///
    /// Valid DATA and domain OPEN content is copied after validation so the
    /// returned frame owns exact-sized backing rather than retaining an
    /// arbitrarily large caller-owned allocation through a small payload or
    /// short domain.
    pub fn try_new(leg_generation: LegGeneration, record: Record) -> Result<Self, ProtocolError> {
        validate_record(&record)?;
        Ok(Self::new(
            leg_generation,
            normalize_record_ownership(record),
        ))
    }

    /// Internal unchecked construction for records that have already crossed
    /// a protocol-validation boundary.
    pub(super) fn new(leg_generation: LegGeneration, record: Record) -> Self {
        Self {
            leg_generation,
            record,
        }
    }

    pub fn leg_generation(&self) -> LegGeneration {
        self.leg_generation
    }

    pub fn record(&self) -> &Record {
        &self.record
    }

    pub fn encode(&self) -> Result<Bytes, ProtocolError> {
        // Internal reducers may use the unchecked constructor after their own
        // preflight. Revalidate here so serialization remains fail-closed.
        validate_record(&self.record)?;
        match &self.record {
            Record::Attach {
                session_id,
                nonce,
                min_version,
                max_version,
                offered_features,
                required_features,
                proof,
            } => {
                validate_negotiation(
                    *min_version,
                    *max_version,
                    *offered_features,
                    *required_features,
                )?;
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + ATTACH_FIXED_BODY_BYTES);
                encode_header(&mut encoded, RECORD_ATTACH, ATTACH_FIXED_BODY_BYTES)?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.extend_from_slice(nonce.as_bytes());
                encoded.put_u16(*min_version);
                encoded.put_u16(*max_version);
                encoded.put_u64(offered_features.bits());
                encoded.put_u64(required_features.bits());
                encoded.extend_from_slice(proof.as_bytes());
                Ok(encoded.freeze())
            }
            Record::AttachAccepted {
                session_id,
                nonce,
                selected_version,
                features,
            } => {
                validate_selected_version(*selected_version)?;
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + ATTACH_ACCEPTED_FIXED_BODY_BYTES);
                encode_header(
                    &mut encoded,
                    RECORD_ATTACH_ACCEPTED,
                    ATTACH_ACCEPTED_FIXED_BODY_BYTES,
                )?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.extend_from_slice(nonce.as_bytes());
                encoded.put_u16(*selected_version);
                encoded.put_u64(features.bits());
                Ok(encoded.freeze())
            }
            Record::AttachGenerationStatus {
                session_id,
                requested_generation,
                nonce,
            } => {
                let mut encoded = BytesMut::with_capacity(
                    FRAME_HEADER_BYTES + ATTACH_GENERATION_STATUS_FIXED_BODY_BYTES,
                );
                encode_header(
                    &mut encoded,
                    RECORD_ATTACH_GENERATION_STATUS,
                    ATTACH_GENERATION_STATUS_FIXED_BODY_BYTES,
                )?;
                encoded.put_u64(self.leg_generation.get());
                encoded.extend_from_slice(session_id.as_bytes());
                encoded.put_u64(requested_generation.get());
                encoded.extend_from_slice(nonce.as_bytes());
                Ok(encoded.freeze())
            }
            Record::Data {
                flow_id,
                direction,
                offset,
                payload,
            } => {
                validate_data_payload(*offset, payload)?;
                let body_len = DATA_FIXED_BODY_BYTES
                    .checked_add(payload.len())
                    .ok_or(ProtocolError::FrameTooLarge)?;
                let body_len = u32::try_from(body_len).map_err(|_| ProtocolError::FrameTooLarge)?;
                let mut encoded = BytesMut::with_capacity(FRAME_HEADER_BYTES + body_len as usize);
                encode_header(&mut encoded, RECORD_DATA, body_len as usize)?;
                encoded.put_u64(self.leg_generation.get());
                encoded.put_u64(flow_id.get());
                encoded.put_u8(direction.encode());
                encoded.put_u64(offset.get());
                encoded.extend_from_slice(payload);
                Ok(encoded.freeze())
            }
            Record::Open { flow_id, target } => {
                let target = encode_target(target)?;
                let body_len = OPEN_PREFIX_BODY_BYTES
                    .checked_add(target.len())
                    .ok_or(ProtocolError::FrameTooLarge)?;
                let body_len = u32::try_from(body_len).map_err(|_| ProtocolError::FrameTooLarge)?;
                let mut encoded = BytesMut::with_capacity(FRAME_HEADER_BYTES + body_len as usize);
                encode_header(&mut encoded, RECORD_OPEN, body_len as usize)?;
                encoded.put_u64(self.leg_generation.get());
                encoded.put_u64(flow_id.get());
                encoded.extend_from_slice(&target);
                Ok(encoded.freeze())
            }
            Record::OpenResult { flow_id, result } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + OPEN_RESULT_FIXED_BODY_BYTES);
                encode_header(
                    &mut encoded,
                    RECORD_OPEN_RESULT,
                    OPEN_RESULT_FIXED_BODY_BYTES,
                )?;
                encoded.put_u64(self.leg_generation.get());
                encoded.put_u64(flow_id.get());
                encoded.put_u16(*result as u16);
                Ok(encoded.freeze())
            }
            Record::Ack {
                flow_id,
                direction,
                next_accepted,
                final_accepted,
            } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + ACK_FIXED_BODY_BYTES);
                encode_header(&mut encoded, RECORD_ACK, ACK_FIXED_BODY_BYTES)?;
                encoded.put_u64(self.leg_generation.get());
                encoded.put_u64(flow_id.get());
                encoded.put_u8(direction.encode());
                encoded.put_u64(next_accepted.get());
                encoded.put_u8(u8::from(*final_accepted));
                Ok(encoded.freeze())
            }
            Record::Close {
                flow_id,
                direction,
                final_offset,
            } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + CLOSE_FIXED_BODY_BYTES);
                encode_header(&mut encoded, RECORD_CLOSE, CLOSE_FIXED_BODY_BYTES)?;
                encoded.put_u64(self.leg_generation.get());
                encoded.put_u64(flow_id.get());
                encoded.put_u8(direction.encode());
                encoded.put_u64(final_offset.get());
                Ok(encoded.freeze())
            }
            Record::Reset { flow_id, reason } => {
                let mut encoded =
                    BytesMut::with_capacity(FRAME_HEADER_BYTES + RESET_FIXED_BODY_BYTES);
                encode_header(&mut encoded, RECORD_RESET, RESET_FIXED_BODY_BYTES)?;
                encoded.put_u64(self.leg_generation.get());
                encoded.put_u64(flow_id.get());
                encoded.put_u16(*reason as u16);
                Ok(encoded.freeze())
            }
        }
    }

    pub fn decode_exact(encoded: &[u8]) -> Result<Self, ProtocolError> {
        let header = ValidatedFrameHeader::decode(encoded)?;
        let total_len = header.total_len()?;
        validate_exact_frame_len(encoded.len(), total_len)?;
        header.decode_compact_body(Bytes::copy_from_slice(&encoded[FRAME_HEADER_BYTES..]))
    }

    pub fn decode_owned_exact(encoded: Bytes) -> Result<Self, ProtocolError> {
        let header = ValidatedFrameHeader::decode(&encoded)?;
        let total_len = header.total_len()?;
        validate_exact_frame_len(encoded.len(), total_len)?;
        // `Bytes` can hide an arbitrarily large backing allocation. Compact
        // the admitted body before it can enter a protocol/session queue.
        header.decode_compact_body(Bytes::copy_from_slice(&encoded[FRAME_HEADER_BYTES..]))
    }
}

fn validate_exact_frame_len(actual: usize, expected: usize) -> Result<(), ProtocolError> {
    if actual < expected {
        return Err(ProtocolError::Truncated {
            needed: expected,
            available: actual,
        });
    }
    if actual != expected {
        return Err(ProtocolError::TrailingBytes { expected, actual });
    }
    Ok(())
}

fn encode_header(
    encoded: &mut BytesMut,
    record_type: u8,
    body_len: usize,
) -> Result<(), ProtocolError> {
    validate_declared_body_len(record_type, body_len)?;
    let body_len = u32::try_from(body_len).map_err(|_| ProtocolError::FrameTooLarge)?;
    encoded.extend_from_slice(&MAGIC);
    encoded.put_u16(FRAME_PROTOCOL_VERSION);
    encoded.put_u8(record_type);
    encoded.put_u8(0);
    encoded.put_u32(body_len);
    Ok(())
}

fn validate_declared_body_len(record_type: u8, body_len: usize) -> Result<(), ProtocolError> {
    let (valid, maximum) = match record_type {
        RECORD_ATTACH => (body_len == ATTACH_FIXED_BODY_BYTES, ATTACH_FIXED_BODY_BYTES),
        RECORD_ATTACH_ACCEPTED => (
            body_len == ATTACH_ACCEPTED_FIXED_BODY_BYTES,
            ATTACH_ACCEPTED_FIXED_BODY_BYTES,
        ),
        RECORD_ATTACH_GENERATION_STATUS => (
            body_len == ATTACH_GENERATION_STATUS_FIXED_BODY_BYTES,
            ATTACH_GENERATION_STATUS_FIXED_BODY_BYTES,
        ),
        RECORD_OPEN => (
            (MIN_OPEN_BODY_BYTES..=MAX_OPEN_BODY_BYTES).contains(&body_len),
            MAX_OPEN_BODY_BYTES,
        ),
        RECORD_OPEN_RESULT => (
            body_len == OPEN_RESULT_FIXED_BODY_BYTES,
            OPEN_RESULT_FIXED_BODY_BYTES,
        ),
        RECORD_DATA => (
            (DATA_FIXED_BODY_BYTES + 1..=DATA_FIXED_BODY_BYTES + MAX_DATA_PAYLOAD_BYTES)
                .contains(&body_len),
            DATA_FIXED_BODY_BYTES + MAX_DATA_PAYLOAD_BYTES,
        ),
        RECORD_ACK => (body_len == ACK_FIXED_BODY_BYTES, ACK_FIXED_BODY_BYTES),
        RECORD_CLOSE => (body_len == CLOSE_FIXED_BODY_BYTES, CLOSE_FIXED_BODY_BYTES),
        RECORD_RESET => (body_len == RESET_FIXED_BODY_BYTES, RESET_FIXED_BODY_BYTES),
        RECORD_STANDBY_REGISTER => (
            body_len == STANDBY_REGISTER_FIXED_BODY_BYTES,
            STANDBY_REGISTER_FIXED_BODY_BYTES,
        ),
        RECORD_STANDBY_ACCEPTED => (
            body_len == STANDBY_ACCEPTED_FIXED_BODY_BYTES,
            STANDBY_ACCEPTED_FIXED_BODY_BYTES,
        ),
        RECORD_LEG_PROBE => (
            body_len == LEG_PROBE_FIXED_BODY_BYTES,
            LEG_PROBE_FIXED_BODY_BYTES,
        ),
        RECORD_LEG_PROBE_ACK => (
            body_len == LEG_PROBE_ACK_FIXED_BODY_BYTES,
            LEG_PROBE_ACK_FIXED_BODY_BYTES,
        ),
        RECORD_SWITCH_HINT => (
            body_len == SWITCH_HINT_FIXED_BODY_BYTES,
            SWITCH_HINT_FIXED_BODY_BYTES,
        ),
        other => return Err(ProtocolError::UnknownRecordType(other)),
    };
    if !valid {
        return Err(if body_len > maximum {
            ProtocolError::FrameTooLarge
        } else {
            ProtocolError::InvalidRecordLength {
                record_type,
                len: body_len,
            }
        });
    }
    Ok(())
}

fn require_standby_control_feature(
    record_type: u8,
    negotiated_features: FeatureSet,
) -> Result<(), ProtocolError> {
    if matches!(
        record_type,
        RECORD_STANDBY_REGISTER
            | RECORD_STANDBY_ACCEPTED
            | RECORD_LEG_PROBE
            | RECORD_LEG_PROBE_ACK
            | RECORD_SWITCH_HINT
    ) {
        require_negotiated_standby_control(negotiated_features)?;
    }
    Ok(())
}

fn require_negotiated_standby_control(
    negotiated_features: FeatureSet,
) -> Result<(), ProtocolError> {
    if !negotiated_features.contains(FeatureSet::STANDBY_CONTROL_V1) {
        return Err(ProtocolError::StandbyControlNotNegotiated {
            negotiated: negotiated_features.bits(),
        });
    }
    Ok(())
}

fn validate_leg_control_record(record: &LegControlRecord) -> Result<(), ProtocolError> {
    match record {
        LegControlRecord::StandbyRegister {
            selected_version,
            features,
            ..
        }
        | LegControlRecord::StandbyAccepted {
            selected_version,
            features,
            ..
        } => {
            validate_selected_version(*selected_version)?;
            if !features.contains(FeatureSet::STANDBY_CONTROL_V1) {
                return Err(ProtocolError::StandbyControlFeatureMissing {
                    features: features.bits(),
                });
            }
            Ok(())
        }
        LegControlRecord::LegProbe { .. } | LegControlRecord::LegProbeAck { .. } => Ok(()),
        LegControlRecord::SwitchHint { direction, .. } => {
            if *direction != Direction::TargetToClient {
                return Err(ProtocolError::InvalidSwitchHintDirection(*direction));
            }
            Ok(())
        }
    }
}

/// Checks every dynamic record invariant without allocating or taking
/// ownership. Non-zero identifiers and closed control values are enforced by
/// their field types before a `Record` can be constructed.
pub(crate) fn validate_record(record: &Record) -> Result<(), ProtocolError> {
    match record {
        Record::Attach {
            min_version,
            max_version,
            offered_features,
            required_features,
            ..
        } => validate_negotiation(
            *min_version,
            *max_version,
            *offered_features,
            *required_features,
        ),
        Record::AttachAccepted {
            selected_version, ..
        } => validate_selected_version(*selected_version),
        Record::Open { target, .. } => validate_target(target),
        Record::Data {
            offset, payload, ..
        } => validate_data_payload(*offset, payload),
        Record::AttachGenerationStatus { .. }
        | Record::OpenResult { .. }
        | Record::Ack { .. }
        | Record::Close { .. }
        | Record::Reset { .. } => Ok(()),
    }
}

fn normalize_record_ownership(record: Record) -> Record {
    match record {
        Record::Data {
            flow_id,
            direction,
            offset,
            payload,
        } => Record::Data {
            flow_id,
            direction,
            offset,
            payload: Bytes::copy_from_slice(&payload),
        },
        Record::Open {
            flow_id,
            target: TargetAddr::DomainPort { host, port },
        } => Record::Open {
            flow_id,
            target: TargetAddr::DomainPort {
                host: Box::<str>::from(host.as_str()).into_string(),
                port,
            },
        },
        // IP targets are fixed-size values with no caller-owned backing.
        record => record,
    }
}

fn validate_target(target: &TargetAddr) -> Result<(), ProtocolError> {
    match target {
        TargetAddr::IpPort(SocketAddr::V4(address)) => validate_target_port(address.port()),
        TargetAddr::IpPort(SocketAddr::V6(address)) => {
            validate_target_port(address.port())?;
            if address.flowinfo() != 0 || address.scope_id() != 0 {
                return Err(ProtocolError::ScopedIpv6Target);
            }
            Ok(())
        }
        TargetAddr::DomainPort { host, port } => {
            validate_target_port(*port)?;
            let host = host.as_bytes();
            if host.is_empty() || host.len() > MAX_DOMAIN_BYTES {
                return Err(ProtocolError::InvalidDomainLength {
                    len: host.len(),
                    max: MAX_DOMAIN_BYTES,
                });
            }
            if host.contains(&0) {
                return Err(ProtocolError::InvalidDomainEncoding);
            }
            Ok(())
        }
    }
}

fn encode_target(target: &TargetAddr) -> Result<Bytes, ProtocolError> {
    validate_target(target)?;
    let mut encoded = BytesMut::new();
    match target {
        TargetAddr::IpPort(SocketAddr::V4(address)) => {
            encoded.put_u8(TARGET_IPV4);
            encoded.put_u16(address.port());
            encoded.extend_from_slice(&address.ip().octets());
        }
        TargetAddr::IpPort(SocketAddr::V6(address)) => {
            encoded.put_u8(TARGET_IPV6);
            encoded.put_u16(address.port());
            encoded.extend_from_slice(&address.ip().octets());
        }
        TargetAddr::DomainPort { host, port } => {
            let host = host.as_bytes();
            encoded.put_u8(TARGET_DOMAIN);
            encoded.put_u16(*port);
            encoded.put_u8(host.len() as u8);
            encoded.extend_from_slice(host);
        }
    }
    Ok(encoded.freeze())
}

fn decode_open(body: Bytes) -> Result<Frame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_OPEN)?)?;
    let flow_id = SessionFlowId::new(read_u64(&body, 8, RECORD_OPEN)?)?;
    let target = decode_target(&body[16..])?;
    Frame::try_new(leg_generation, Record::Open { flow_id, target })
}

fn decode_target(encoded: &[u8]) -> Result<TargetAddr, ProtocolError> {
    let target_type = *encoded.first().ok_or(ProtocolError::InvalidRecordLength {
        record_type: RECORD_OPEN,
        len: encoded.len(),
    })?;
    let port = read_u16(encoded, 1, RECORD_OPEN)?;
    validate_target_port(port)?;
    match target_type {
        TARGET_IPV4 if encoded.len() == 7 => {
            let octets = read_array::<4>(encoded, 3, RECORD_OPEN)?;
            Ok(TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(octets),
                port,
            ))))
        }
        TARGET_IPV6 if encoded.len() == 19 => {
            let octets = read_array::<16>(encoded, 3, RECORD_OPEN)?;
            Ok(TargetAddr::IpPort(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(octets),
                port,
                0,
                0,
            ))))
        }
        TARGET_DOMAIN => {
            let len = usize::from(*encoded.get(3).ok_or(ProtocolError::InvalidRecordLength {
                record_type: RECORD_OPEN,
                len: encoded.len(),
            })?);
            if len == 0 || len > MAX_DOMAIN_BYTES || encoded.len() != 4 + len {
                return Err(ProtocolError::InvalidDomainLength {
                    len,
                    max: MAX_DOMAIN_BYTES,
                });
            }
            let host = std::str::from_utf8(&encoded[4..])
                .map_err(|_| ProtocolError::InvalidDomainEncoding)?;
            if host.as_bytes().contains(&0) {
                return Err(ProtocolError::InvalidDomainEncoding);
            }
            Ok(TargetAddr::DomainPort {
                host: host.to_owned(),
                port,
            })
        }
        TARGET_IPV4 | TARGET_IPV6 => Err(ProtocolError::InvalidRecordLength {
            record_type: RECORD_OPEN,
            len: encoded.len(),
        }),
        other => Err(ProtocolError::InvalidTargetType(other)),
    }
}

fn validate_target_port(port: u16) -> Result<(), ProtocolError> {
    if port == 0 {
        return Err(ProtocolError::ZeroTargetPort);
    }
    Ok(())
}

fn decode_attach(body: Bytes) -> Result<Frame, ProtocolError> {
    if body.len() != ATTACH_FIXED_BODY_BYTES {
        return Err(ProtocolError::InvalidRecordLength {
            record_type: RECORD_ATTACH,
            len: body.len(),
        });
    }
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_ATTACH)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_ATTACH)?)?;
    let nonce = AttachNonce::new(read_array(&body, 24, RECORD_ATTACH)?)?;
    let min_version = read_u16(&body, 40, RECORD_ATTACH)?;
    let max_version = read_u16(&body, 42, RECORD_ATTACH)?;
    let offered_features = FeatureSet::new(read_u64(&body, 44, RECORD_ATTACH)?);
    let required_features = FeatureSet::new(read_u64(&body, 52, RECORD_ATTACH)?);
    validate_negotiation(
        min_version,
        max_version,
        offered_features,
        required_features,
    )?;
    let proof = AttachProof::new(read_array(&body, 60, RECORD_ATTACH)?)?;
    Ok(Frame::new(
        leg_generation,
        Record::Attach {
            session_id,
            nonce,
            min_version,
            max_version,
            offered_features,
            required_features,
            proof,
        },
    ))
}

fn decode_attach_accepted(body: Bytes) -> Result<Frame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_ATTACH_ACCEPTED)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_ATTACH_ACCEPTED)?)?;
    let nonce = AttachNonce::new(read_array(&body, 24, RECORD_ATTACH_ACCEPTED)?)?;
    let selected_version = read_u16(&body, 40, RECORD_ATTACH_ACCEPTED)?;
    validate_selected_version(selected_version)?;
    let features = FeatureSet::new(read_u64(&body, 42, RECORD_ATTACH_ACCEPTED)?);
    Ok(Frame::new(
        leg_generation,
        Record::AttachAccepted {
            session_id,
            nonce,
            selected_version,
            features,
        },
    ))
}

fn decode_attach_generation_status(body: Bytes) -> Result<Frame, ProtocolError> {
    let current_generation =
        LegGeneration::new(read_u64(&body, 0, RECORD_ATTACH_GENERATION_STATUS)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_ATTACH_GENERATION_STATUS)?)?;
    let requested_generation =
        LegGeneration::new(read_u64(&body, 24, RECORD_ATTACH_GENERATION_STATUS)?)?;
    let nonce = AttachNonce::new(read_array(&body, 32, RECORD_ATTACH_GENERATION_STATUS)?)?;
    Ok(Frame::new(
        current_generation,
        Record::AttachGenerationStatus {
            session_id,
            requested_generation,
            nonce,
        },
    ))
}

fn decode_standby_register(body: Bytes) -> Result<LegControlFrame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_STANDBY_REGISTER)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_STANDBY_REGISTER)?)?;
    let standby_nonce = StandbyNonce::new(read_array(&body, 24, RECORD_STANDBY_REGISTER)?)?;
    let selected_version = read_u16(&body, 40, RECORD_STANDBY_REGISTER)?;
    let features = FeatureSet::new(read_u64(&body, 42, RECORD_STANDBY_REGISTER)?);
    let proof = StandbyProof::new(read_array(&body, 50, RECORD_STANDBY_REGISTER)?)?;
    let record = LegControlRecord::StandbyRegister {
        session_id,
        standby_nonce,
        selected_version,
        features,
        proof,
    };
    validate_leg_control_record(&record)?;
    Ok(LegControlFrame::new(leg_generation, record))
}

fn decode_standby_accepted(body: Bytes) -> Result<LegControlFrame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_STANDBY_ACCEPTED)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_STANDBY_ACCEPTED)?)?;
    let standby_nonce = StandbyNonce::new(read_array(&body, 24, RECORD_STANDBY_ACCEPTED)?)?;
    let selected_version = read_u16(&body, 40, RECORD_STANDBY_ACCEPTED)?;
    let features = FeatureSet::new(read_u64(&body, 42, RECORD_STANDBY_ACCEPTED)?);
    let record = LegControlRecord::StandbyAccepted {
        session_id,
        standby_nonce,
        selected_version,
        features,
    };
    validate_leg_control_record(&record)?;
    Ok(LegControlFrame::new(leg_generation, record))
}

fn decode_leg_probe(body: Bytes) -> Result<LegControlFrame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_LEG_PROBE)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_LEG_PROBE)?)?;
    let leg_epoch_nonce = LegEpochNonce::new(read_array(&body, 24, RECORD_LEG_PROBE)?)?;
    let probe_sequence = ProbeSequence::new(read_u64(&body, 40, RECORD_LEG_PROBE)?)?;
    Ok(LegControlFrame::new(
        leg_generation,
        LegControlRecord::LegProbe {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
    ))
}

fn decode_leg_probe_ack(body: Bytes) -> Result<LegControlFrame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_LEG_PROBE_ACK)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_LEG_PROBE_ACK)?)?;
    let leg_epoch_nonce = LegEpochNonce::new(read_array(&body, 24, RECORD_LEG_PROBE_ACK)?)?;
    let probe_sequence = ProbeSequence::new(read_u64(&body, 40, RECORD_LEG_PROBE_ACK)?)?;
    Ok(LegControlFrame::new(
        leg_generation,
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
    ))
}

fn decode_switch_hint(body: Bytes) -> Result<LegControlFrame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_SWITCH_HINT)?)?;
    let session_id = SessionId::new(read_array(&body, 8, RECORD_SWITCH_HINT)?)?;
    let standby_nonce = StandbyNonce::new(read_array(&body, 24, RECORD_SWITCH_HINT)?)?;
    let hint_sequence = HintSequence::new(read_u64(&body, 40, RECORD_SWITCH_HINT)?)?;
    let flow_id = SessionFlowId::new(read_u64(&body, 48, RECORD_SWITCH_HINT)?)?;
    let direction = Direction::decode(body[56])?;
    let oldest_unacknowledged = ByteOffset::new(read_u64(&body, 57, RECORD_SWITCH_HINT)?);
    let cause = SwitchHintCause::decode(read_u16(&body, 65, RECORD_SWITCH_HINT)?)?;
    let record = LegControlRecord::SwitchHint {
        session_id,
        standby_nonce,
        hint_sequence,
        flow_id,
        direction,
        oldest_unacknowledged,
        cause,
    };
    validate_leg_control_record(&record)?;
    Ok(LegControlFrame::new(leg_generation, record))
}

fn validate_negotiation(
    min_version: u16,
    max_version: u16,
    offered_features: FeatureSet,
    required_features: FeatureSet,
) -> Result<(), ProtocolError> {
    if min_version == 0 || min_version > max_version {
        return Err(ProtocolError::InvalidVersionRange {
            min: min_version,
            max: max_version,
        });
    }
    if !offered_features.contains(required_features) {
        return Err(ProtocolError::RequiredFeaturesNotOffered {
            offered: offered_features.bits(),
            required: required_features.bits(),
        });
    }
    Ok(())
}

fn validate_selected_version(selected_version: u16) -> Result<(), ProtocolError> {
    if selected_version == 0 {
        return Err(ProtocolError::InvalidVersionRange {
            min: selected_version,
            max: selected_version,
        });
    }
    Ok(())
}

fn decode_open_result(body: Bytes) -> Result<Frame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_OPEN_RESULT)?)?;
    let flow_id = SessionFlowId::new(read_u64(&body, 8, RECORD_OPEN_RESULT)?)?;
    let result = OpenResultCode::decode(read_u16(&body, 16, RECORD_OPEN_RESULT)?)?;
    Ok(Frame::new(
        leg_generation,
        Record::OpenResult { flow_id, result },
    ))
}

fn decode_ack(body: Bytes) -> Result<Frame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_ACK)?)?;
    let flow_id = SessionFlowId::new(read_u64(&body, 8, RECORD_ACK)?)?;
    let direction = Direction::decode(body[16])?;
    let next_accepted = ByteOffset::new(read_u64(&body, 17, RECORD_ACK)?);
    let final_accepted = decode_bool(body[25])?;
    Ok(Frame::new(
        leg_generation,
        Record::Ack {
            flow_id,
            direction,
            next_accepted,
            final_accepted,
        },
    ))
}

fn decode_close(body: Bytes) -> Result<Frame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_CLOSE)?)?;
    let flow_id = SessionFlowId::new(read_u64(&body, 8, RECORD_CLOSE)?)?;
    let direction = Direction::decode(body[16])?;
    let final_offset = ByteOffset::new(read_u64(&body, 17, RECORD_CLOSE)?);
    Ok(Frame::new(
        leg_generation,
        Record::Close {
            flow_id,
            direction,
            final_offset,
        },
    ))
}

fn decode_reset(body: Bytes) -> Result<Frame, ProtocolError> {
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_RESET)?)?;
    let flow_id = SessionFlowId::new(read_u64(&body, 8, RECORD_RESET)?)?;
    let reason = ResetReason::decode(read_u16(&body, 16, RECORD_RESET)?)?;
    Ok(Frame::new(
        leg_generation,
        Record::Reset { flow_id, reason },
    ))
}

fn decode_bool(value: u8) -> Result<bool, ProtocolError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(ProtocolError::InvalidBoolean(other)),
    }
}

fn validate_data_payload(offset: ByteOffset, payload: &[u8]) -> Result<(), ProtocolError> {
    if payload.is_empty() {
        return Err(ProtocolError::EmptyData);
    }
    if payload.len() > MAX_DATA_PAYLOAD_BYTES {
        return Err(ProtocolError::DataTooLarge {
            len: payload.len(),
            max: MAX_DATA_PAYLOAD_BYTES,
        });
    }
    offset.checked_advance(payload.len())?;
    Ok(())
}

fn decode_data(body: Bytes) -> Result<Frame, ProtocolError> {
    if body.len() < DATA_FIXED_BODY_BYTES {
        return Err(ProtocolError::InvalidRecordLength {
            record_type: RECORD_DATA,
            len: body.len(),
        });
    }
    let leg_generation = LegGeneration::new(read_u64(&body, 0, RECORD_DATA)?)?;
    let flow_id = SessionFlowId::new(read_u64(&body, 8, RECORD_DATA)?)?;
    let direction = Direction::decode(body[16])?;
    let offset = ByteOffset::new(read_u64(&body, 17, RECORD_DATA)?);
    let payload = body.slice(DATA_FIXED_BODY_BYTES..);
    validate_data_payload(offset, &payload)?;
    Ok(Frame::new(
        leg_generation,
        Record::Data {
            flow_id,
            direction,
            offset,
            payload,
        },
    ))
}

fn read_u16(input: &[u8], offset: usize, record_type: u8) -> Result<u16, ProtocolError> {
    let value = input
        .get(offset..offset + 2)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ProtocolError::InvalidRecordLength {
            record_type,
            len: input.len(),
        })?;
    Ok(u16::from_be_bytes(value))
}

fn read_u64(input: &[u8], offset: usize, record_type: u8) -> Result<u64, ProtocolError> {
    let value = input
        .get(offset..offset + 8)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ProtocolError::InvalidRecordLength {
            record_type,
            len: input.len(),
        })?;
    Ok(u64::from_be_bytes(value))
}

fn read_array<const N: usize>(
    input: &[u8],
    offset: usize,
    record_type: u8,
) -> Result<[u8; N], ProtocolError> {
    input
        .get(offset..offset + N)
        .ok_or(ProtocolError::InvalidRecordLength {
            record_type,
            len: input.len(),
        })?
        .try_into()
        .map_err(|_| ProtocolError::InvalidRecordLength {
            record_type,
            len: input.len(),
        })
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("protocol frame magic is invalid")]
    InvalidMagic,
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u16),
    #[error("unknown protocol record type {0}")]
    UnknownRecordType(u8),
    #[error("protocol flags must be zero for version 1, got {0:#04x}")]
    NonZeroFlags(u8),
    #[error("protocol frame is truncated: need {needed} bytes, have {available}")]
    Truncated { needed: usize, available: usize },
    #[error("protocol frame has trailing bytes: expected {expected}, got {actual}")]
    TrailingBytes { expected: usize, actual: usize },
    #[error("protocol frame length exceeds representable bounds")]
    FrameTooLarge,
    #[error("record type {record_type} has invalid body length {len}")]
    InvalidRecordLength { record_type: u8, len: usize },
    #[error("flow id zero is reserved")]
    ZeroFlowId,
    #[error("session id zero is reserved")]
    ZeroSessionId,
    #[error("attach nonce zero is reserved")]
    ZeroAttachNonce,
    #[error("attach proof zero is reserved")]
    ZeroAttachProof,
    #[error("standby registration nonce zero is reserved")]
    ZeroStandbyNonce,
    #[error("leg epoch nonce zero is reserved")]
    ZeroLegEpochNonce,
    #[error("probe sequence zero is reserved")]
    ZeroProbeSequence,
    #[error("probe sequence overflow")]
    ProbeSequenceOverflow,
    #[error("switch-hint sequence zero is reserved")]
    ZeroHintSequence,
    #[error("switch-hint sequence overflow")]
    HintSequenceOverflow,
    #[error("standby registration proof zero is reserved")]
    ZeroStandbyProof,
    #[error("leg generation zero is reserved")]
    ZeroLegGeneration,
    #[error("invalid TCP direction {0}")]
    InvalidDirection(u8),
    #[error("TCP DATA payload must not be empty")]
    EmptyData,
    #[error("TCP DATA payload length {len} exceeds {max}")]
    DataTooLarge { len: usize, max: usize },
    #[error("TCP byte offset overflow")]
    OffsetOverflow,
    #[error("target port zero is reserved")]
    ZeroTargetPort,
    #[error("target address type {0} is invalid")]
    InvalidTargetType(u8),
    #[error("target domain length {len} is invalid; maximum is {max}")]
    InvalidDomainLength { len: usize, max: usize },
    #[error("target domain is not valid protocol UTF-8 or contains NUL")]
    InvalidDomainEncoding,
    #[error("scoped IPv6 targets cannot cross the Upstream boundary")]
    ScopedIpv6Target,
    #[error("invalid protocol version range {min}..={max}")]
    InvalidVersionRange { min: u16, max: u16 },
    #[error("required feature bits {required:#018x} are not in offered bits {offered:#018x}")]
    RequiredFeaturesNotOffered { offered: u64, required: u64 },
    #[error("invalid OPEN_RESULT code {0}")]
    InvalidOpenResult(u16),
    #[error("invalid RESET reason {0}")]
    InvalidResetReason(u16),
    #[error("invalid SWITCH_HINT cause {0}")]
    InvalidSwitchHintCause(u16),
    #[error("SWITCH_HINT direction must be TargetToClient, got {0:?}")]
    InvalidSwitchHintDirection(Direction),
    #[error("invalid boolean encoding {0}")]
    InvalidBoolean(u8),
    #[error("STANDBY_CONTROL_V1 was not negotiated; negotiated bits {negotiated:#018x}")]
    StandbyControlNotNegotiated { negotiated: u64 },
    #[error("standby-control record omits STANDBY_CONTROL_V1; record bits {features:#018x}")]
    StandbyControlFeatureMissing { features: u64 },
    #[error("leg-control records require classified decoding")]
    LegControlRequiresClassification,
}

#[cfg(test)]
mod standby_control_tests {
    use super::*;

    #[test]
    fn standby_register_round_trips_through_the_feature_gated_classifier() {
        let negotiated = FeatureSet::STANDBY_CONTROL_V1;
        let frame = LegControlFrame::try_new(
            LegGeneration::new(2).unwrap(),
            LegControlRecord::StandbyRegister {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features: negotiated,
                proof: StandbyProof::new([0x44; 32]).unwrap(),
            },
            negotiated,
        )
        .unwrap();

        let encoded = frame.encode(negotiated).unwrap();
        assert_eq!(encoded.len(), 94);
        assert_eq!(encoded[6], 0x0a);
        assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 82);
        assert_eq!(
            DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
            DecodedFrame::LegControl(frame)
        );
    }

    #[test]
    fn standby_accepted_round_trips_with_the_exact_registered_contract() {
        let negotiated = FeatureSet::STANDBY_CONTROL_V1;
        let frame = LegControlFrame::try_new(
            LegGeneration::new(2).unwrap(),
            LegControlRecord::StandbyAccepted {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features: negotiated,
            },
            negotiated,
        )
        .unwrap();

        let encoded = frame.encode(negotiated).unwrap();
        assert_eq!(encoded.len(), 62);
        assert_eq!(encoded[6], 0x0b);
        assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 50);
        assert_eq!(
            DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
            DecodedFrame::LegControl(frame)
        );
    }

    #[test]
    fn leg_probe_round_trips_with_nonzero_epoch_and_sequence() {
        let negotiated = FeatureSet::STANDBY_CONTROL_V1;
        let frame = LegControlFrame::try_new(
            LegGeneration::new(2).unwrap(),
            LegControlRecord::LegProbe {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                leg_epoch_nonce: LegEpochNonce::new([0x77; 16]).unwrap(),
                probe_sequence: ProbeSequence::new(9).unwrap(),
            },
            negotiated,
        )
        .unwrap();

        let encoded = frame.encode(negotiated).unwrap();
        assert_eq!(encoded.len(), 60);
        assert_eq!(encoded[6], 0x0c);
        assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 48);
        assert_eq!(
            DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
            DecodedFrame::LegControl(frame)
        );
    }

    #[test]
    fn leg_probe_ack_round_trips_the_exact_probe_correlation() {
        let negotiated = FeatureSet::STANDBY_CONTROL_V1;
        let frame = LegControlFrame::try_new(
            LegGeneration::new(2).unwrap(),
            LegControlRecord::LegProbeAck {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                leg_epoch_nonce: LegEpochNonce::new([0x77; 16]).unwrap(),
                probe_sequence: ProbeSequence::new(9).unwrap(),
            },
            negotiated,
        )
        .unwrap();

        let encoded = frame.encode(negotiated).unwrap();
        assert_eq!(encoded.len(), 60);
        assert_eq!(encoded[6], 0x0d);
        assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 48);
        assert_eq!(
            DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
            DecodedFrame::LegControl(frame)
        );
    }

    #[test]
    fn switch_hint_round_trips_the_closed_reverse_stall_evidence() {
        let negotiated = FeatureSet::STANDBY_CONTROL_V1;
        let frame = LegControlFrame::try_new(
            LegGeneration::new(2).unwrap(),
            LegControlRecord::SwitchHint {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
                hint_sequence: HintSequence::new(5).unwrap(),
                flow_id: SessionFlowId::new(7).unwrap(),
                direction: Direction::TargetToClient,
                oldest_unacknowledged: ByteOffset::new(1024),
                cause: SwitchHintCause::ReverseApplicationAckStall,
            },
            negotiated,
        )
        .unwrap();

        let encoded = frame.encode(negotiated).unwrap();
        assert_eq!(encoded.len(), 79);
        assert_eq!(encoded[6], 0x0e);
        assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 67);
        assert_eq!(
            DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
            DecodedFrame::LegControl(frame)
        );
    }
}
