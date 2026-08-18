//! Byte-level transport boundary for one authenticated resumable leg.
//!
//! The adapter is deliberately the only path from a protocol [`Frame`] to
//! [`TwoLegWire`] and from an [`EncodedDelivery`] to a [`LegBoundFrame`].
//! Fault injection therefore observes and mutates only encoded bytes.  A
//! successfully decoded frame receives provenance from this endpoint's exact
//! process-local [`EstablishedLeg`] and from no numeric leg identifier.

use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use thiserror::Error;

use crate::owned_upstream::leg::{EstablishedLeg, LegBoundFrame};
use crate::owned_upstream::two_leg::{
    EncodedDelivery, FaultAction, LegId, SimTime, TwoLegWire, WireCapacity, WireCounters,
    WireDirection, WireError, WireLane, WireRoute,
};
use crate::resumable::{AttachTransportBinding, Frame, ProtocolError, Record};

/// Local endpoint role for one authenticated transport connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LegEndpointRole {
    Client,
    Owner,
}

impl LegEndpointRole {
    const fn outbound_direction(self) -> WireDirection {
        match self {
            Self::Client => WireDirection::ClientToOwner,
            Self::Owner => WireDirection::OwnerToClient,
        }
    }

    const fn inbound_direction(self) -> WireDirection {
        match self {
            Self::Client => WireDirection::OwnerToClient,
            Self::Owner => WireDirection::ClientToOwner,
        }
    }
}

/// Finite lifetime work admitted by one local leg endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LegIoLimits {
    max_frame_bytes: u64,
    max_outbound_messages: u64,
    max_outbound_bytes: u64,
    max_inbound_messages: u64,
    max_inbound_bytes: u64,
}

impl LegIoLimits {
    pub(crate) fn new(
        max_frame_bytes: usize,
        max_outbound_messages: u64,
        max_outbound_bytes: u64,
        max_inbound_messages: u64,
        max_inbound_bytes: u64,
    ) -> Result<Self, LegIoConfigError> {
        let max_frame_bytes = u64::try_from(max_frame_bytes)
            .map_err(|_| LegIoConfigError::PlatformCapacityOverflow)?;
        if max_frame_bytes == 0 {
            return Err(LegIoConfigError::ZeroFrameBound);
        }
        if max_outbound_messages == 0 || max_inbound_messages == 0 {
            return Err(LegIoConfigError::ZeroMessageBudget);
        }
        if max_outbound_bytes < max_frame_bytes || max_inbound_bytes < max_frame_bytes {
            return Err(LegIoConfigError::FrameExceedsByteBudget);
        }
        Ok(Self {
            max_frame_bytes,
            max_outbound_messages,
            max_outbound_bytes,
            max_inbound_messages,
            max_inbound_bytes,
        })
    }

    pub(crate) fn from_wire_capacity(
        wire_capacity: WireCapacity,
        max_frame_bytes: usize,
    ) -> Result<Self, LegIoConfigError> {
        Self::new(
            max_frame_bytes,
            wire_capacity.max_send_messages(),
            wire_capacity.max_send_bytes(),
            wire_capacity.max_delivery_messages(),
            wire_capacity.max_delivery_bytes(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub(crate) enum LegIoConfigError {
    #[error("leg frame bound must be nonzero")]
    ZeroFrameBound,
    #[error("leg message budgets must be nonzero")]
    ZeroMessageBudget,
    #[error("leg frame bound exceeds a directional byte budget")]
    FrameExceedsByteBudget,
    #[error("leg I/O capacity exceeds platform limits")]
    PlatformCapacityOverflow,
    #[error("leg outbound frame queue must admit at least one frame")]
    ZeroOutboundQueueFrames,
    #[error("leg outbound frame queue must own at least one byte")]
    ZeroOutboundQueueBytes,
}

/// Exact, finite accounting for one endpoint.  Each receive rejection is a
/// subset of `inbound_messages`, so adversarial malformed input cannot grow a
/// diagnostic counter beyond the configured lifetime budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LegIoCounters {
    pub(crate) outbound_messages: u64,
    pub(crate) outbound_bytes: u64,
    pub(crate) outbound_control_messages: u64,
    pub(crate) outbound_data_messages: u64,
    pub(crate) inbound_messages: u64,
    pub(crate) inbound_bytes: u64,
    pub(crate) bound_messages: u64,
    pub(crate) bound_bytes: u64,
    pub(crate) wrong_route_rejections: u64,
    pub(crate) wrong_lane_rejections: u64,
    pub(crate) decode_rejections: u64,
}

/// One non-cloneable local endpoint of an authenticated leg.
pub(crate) struct LegIo {
    leg_id: LegId,
    role: LegEndpointRole,
    established: EstablishedLeg,
    limits: LegIoLimits,
    counters: LegIoCounters,
}

/// Controller-visible byte transport.  It intentionally exposes neither
/// fault configuration nor physical/drop outcomes.
pub(crate) trait EncodedLegTransport {
    fn send_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
    ) -> Result<(), EncodedLegTransportError>;
}

/// Bounded production-shared owner of encoded protocol frames awaiting one
/// leg submission. A failed transport call leaves the exact frame at the
/// front, so DATA and control effects cannot be consumed by transient
/// pressure before the transport accepts their bytes.
pub(crate) struct LegOutboundQueue {
    max_frames: usize,
    max_bytes: usize,
    owned_bytes: usize,
    frames: VecDeque<(Frame, usize)>,
}

impl LegOutboundQueue {
    pub(crate) fn new(max_frames: usize, max_bytes: usize) -> Result<Self, LegIoConfigError> {
        if max_frames == 0 {
            return Err(LegIoConfigError::ZeroOutboundQueueFrames);
        }
        if max_bytes == 0 {
            return Err(LegIoConfigError::ZeroOutboundQueueBytes);
        }
        Ok(Self {
            max_frames,
            max_bytes,
            owned_bytes: 0,
            frames: VecDeque::new(),
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "the error must return the exact unconsumed frame without a hot-path allocation"
    )]
    pub(crate) fn push(&mut self, frame: Frame) -> Result<(), LegOutboundQueueError> {
        let bytes = match frame.encode() {
            Ok(encoded) => encoded.len(),
            Err(source) => {
                return Err(LegOutboundQueueError::new(
                    frame,
                    LegOutboundQueueErrorKind::InvalidFrame(source),
                ));
            }
        };
        let Some(next_bytes) = self.owned_bytes.checked_add(bytes) else {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::ByteCountOverflow,
            ));
        };
        if self.frames.len() >= self.max_frames || next_bytes > self.max_bytes {
            return Err(LegOutboundQueueError::new(
                frame,
                LegOutboundQueueErrorKind::CapacityExceeded {
                    queued_frames: self.frames.len(),
                    queued_bytes: self.owned_bytes,
                    incoming_bytes: bytes,
                },
            ));
        }
        self.frames.push_back((frame, bytes));
        self.owned_bytes = next_bytes;
        Ok(())
    }

    pub(crate) fn try_flush<T: EncodedLegTransport + ?Sized>(
        &mut self,
        endpoint: &mut LegIo,
        transport: &mut T,
        now: SimTime,
    ) -> Result<Option<OutboundSubmission>, LegIoError> {
        let Some((frame, _)) = self.frames.front() else {
            return Ok(None);
        };
        let is_ack = matches!(frame.record(), Record::Ack { .. });
        endpoint.send_retained_frame(transport, now, frame)?;
        let (_, bytes) = self.frames.pop_front().ok_or(LegIoError::CounterOverflow)?;
        self.owned_bytes = self
            .owned_bytes
            .checked_sub(bytes)
            .ok_or(LegIoError::CounterOverflow)?;
        Ok(Some(OutboundSubmission { is_ack }))
    }

    pub(crate) fn len(&self) -> usize {
        self.frames.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub(crate) const fn owned_bytes(&self) -> usize {
        self.owned_bytes
    }
}

impl fmt::Debug for LegOutboundQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegOutboundQueue")
            .field("max_frames", &self.max_frames)
            .field("max_bytes", &self.max_bytes)
            .field("queued_frames", &self.len())
            .field("owned_bytes", &self.owned_bytes)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutboundSubmission {
    pub(crate) is_ack: bool,
}

/// Non-cloneable enqueue rejection that returns the exact unconsumed frame to
/// its producer. Debug deliberately omits the frame and all record content.
pub(crate) struct LegOutboundQueueError {
    kind: LegOutboundQueueErrorKind,
    frame: Frame,
}

impl LegOutboundQueueError {
    fn new(frame: Frame, kind: LegOutboundQueueErrorKind) -> Self {
        Self { kind, frame }
    }

    pub(crate) const fn kind(&self) -> &LegOutboundQueueErrorKind {
        &self.kind
    }

    pub(crate) fn into_frame(self) -> Frame {
        self.frame
    }
}

impl fmt::Debug for LegOutboundQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegOutboundQueueError")
            .field("kind", &self.kind)
            .field("frame", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for LegOutboundQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            LegOutboundQueueErrorKind::InvalidFrame(source) => {
                write!(formatter, "outbound frame is invalid: {source}")
            }
            LegOutboundQueueErrorKind::ByteCountOverflow => {
                formatter.write_str("outbound frame byte ownership overflow")
            }
            LegOutboundQueueErrorKind::CapacityExceeded {
                queued_frames,
                queued_bytes,
                incoming_bytes,
            } => write!(
                formatter,
                "outbound frame queue capacity exceeded: {queued_frames} frames/{queued_bytes} bytes queued, incoming {incoming_bytes} bytes"
            ),
        }
    }
}

impl std::error::Error for LegOutboundQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            LegOutboundQueueErrorKind::InvalidFrame(source) => Some(source),
            LegOutboundQueueErrorKind::ByteCountOverflow
            | LegOutboundQueueErrorKind::CapacityExceeded { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LegOutboundQueueErrorKind {
    InvalidFrame(ProtocolError),
    ByteCountOverflow,
    CapacityExceeded {
        queued_frames: usize,
        queued_bytes: usize,
        incoming_bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MemoryFaultDirective {
    route: WireRoute,
    lane: WireLane,
    action: FaultAction,
}

/// Finite, preconfigured fault schedule owned by the deterministic memory
/// orchestrator, never by a session controller.
pub(crate) struct MemoryFaultScript {
    max_directives: usize,
    directives: BTreeMap<u64, MemoryFaultDirective>,
}

impl MemoryFaultScript {
    pub(crate) fn new(max_directives: usize) -> Self {
        Self {
            max_directives,
            directives: BTreeMap::new(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        submission_ordinal: u64,
        route: WireRoute,
        lane: WireLane,
        action: FaultAction,
    ) -> Result<(), MemoryFaultScriptError> {
        if self.directives.contains_key(&submission_ordinal) {
            return Err(MemoryFaultScriptError::DuplicateOrdinal);
        }
        if self.directives.len() >= self.max_directives {
            return Err(MemoryFaultScriptError::CapacityExceeded);
        }
        self.directives.insert(
            submission_ordinal,
            MemoryFaultDirective {
                route,
                lane,
                action,
            },
        );
        Ok(())
    }
}

impl fmt::Debug for MemoryFaultScript {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryFaultScript")
            .field("configured_directives", &self.directives.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub(crate) enum MemoryFaultScriptError {
    #[error("memory fault-script capacity exceeded")]
    CapacityExceeded,
    #[error("memory fault-script submission ordinal is duplicated")]
    DuplicateOrdinal,
}

/// Byte-only deterministic adapter.  Controllers receive this only through
/// [`EncodedLegTransport`]; the test orchestrator configures the fault script
/// before constructing it and separately drives delivery/virtual time.
pub(crate) struct MemoryLegTransport {
    wire: TwoLegWire,
    fault_script: MemoryFaultScript,
    next_submission_ordinal: u64,
}

impl MemoryLegTransport {
    pub(crate) fn new(wire: TwoLegWire, fault_script: MemoryFaultScript) -> Self {
        Self {
            wire,
            fault_script,
            next_submission_ordinal: 0,
        }
    }

    pub(crate) fn advance_one_due(&mut self, through: SimTime) -> Result<bool, WireError> {
        self.wire.advance_one_due(through)
    }

    pub(crate) fn advance_idle_to(&mut self, deadline: SimTime) -> Result<(), WireError> {
        self.wire.advance_idle_to(deadline)
    }

    pub(crate) fn release_hold(
        &mut self,
        token: u64,
        release_at: SimTime,
    ) -> Result<(), WireError> {
        self.wire.release_hold(token, release_at)
    }

    pub(crate) fn try_recv_next(
        &mut self,
        route: WireRoute,
    ) -> Result<Option<EncodedDelivery>, WireError> {
        self.wire.try_recv_next(route)
    }

    pub(crate) const fn wire_counters(&self) -> WireCounters {
        self.wire.counters()
    }

    pub(crate) fn remaining_fault_directives(&self) -> usize {
        self.fault_script.directives.len()
    }

    /// Borrows only the controller-visible transmit capability.  The returned
    /// view has no fault, delivery, virtual-time, or physical-counter API.
    pub(crate) fn controller_sender(&mut self) -> MemoryLegSender<'_> {
        MemoryLegSender { transport: self }
    }

    /// Raw byte injection exists only for transport-boundary fail-closed unit
    /// tests.  Session/controller code has no matching trait operation.
    #[cfg(test)]
    fn inject_encoded_test_message(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
    ) -> Result<(), WireError> {
        self.wire
            .send(now, route, lane, bytes, FaultAction::Pass)
            .map(|_| ())
    }
}

/// Narrow controller handle into the deterministic memory transport.
pub(crate) struct MemoryLegSender<'a> {
    transport: &'a mut MemoryLegTransport,
}

impl EncodedLegTransport for MemoryLegSender<'_> {
    fn send_encoded(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
    ) -> Result<(), EncodedLegTransportError> {
        let ordinal = self.transport.next_submission_ordinal;
        let next_ordinal = ordinal
            .checked_add(1)
            .ok_or(EncodedLegTransportError::SubmissionOrdinalExhausted)?;
        let action = if let Some(directive) = self.transport.fault_script.directives.get(&ordinal) {
            if directive.route != route || directive.lane != lane {
                return Err(EncodedLegTransportError::FaultScriptMismatch {
                    ordinal,
                    expected_route: directive.route,
                    actual_route: route,
                    expected_lane: directive.lane,
                    actual_lane: lane,
                });
            }
            directive.action
        } else {
            FaultAction::Pass
        };

        self.transport
            .wire
            .send(now, route, lane, bytes, action)
            .map_err(EncodedLegTransportError::Wire)?;
        self.transport.fault_script.directives.remove(&ordinal);
        self.transport.next_submission_ordinal = next_ordinal;
        Ok(())
    }
}

impl fmt::Debug for MemoryLegSender<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemoryLegSender([REDACTED])")
    }
}

impl fmt::Debug for MemoryLegTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryLegTransport")
            .field("next_submission_ordinal", &self.next_submission_ordinal)
            .field(
                "remaining_fault_directives",
                &self.remaining_fault_directives(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum EncodedLegTransportError {
    #[error("encoded leg wire operation failed: {0}")]
    Wire(WireError),
    #[error(
        "fault script mismatch at submission {ordinal}: expected {expected_route:?}/{expected_lane:?}, got {actual_route:?}/{actual_lane:?}"
    )]
    FaultScriptMismatch {
        ordinal: u64,
        expected_route: WireRoute,
        actual_route: WireRoute,
        expected_lane: WireLane,
        actual_lane: WireLane,
    },
    #[error("encoded leg submission ordinal exhausted")]
    SubmissionOrdinalExhausted,
}

impl LegIo {
    pub(crate) fn for_authenticated_transport(
        leg_id: LegId,
        role: LegEndpointRole,
        binding: AttachTransportBinding,
        limits: LegIoLimits,
    ) -> Self {
        Self {
            leg_id,
            role,
            established: EstablishedLeg::for_authenticated_transport(binding),
            limits,
            counters: LegIoCounters::default(),
        }
    }

    pub(crate) const fn outbound_route(&self) -> WireRoute {
        WireRoute::new(self.leg_id, self.role.outbound_direction())
    }

    pub(crate) const fn inbound_route(&self) -> WireRoute {
        WireRoute::new(self.leg_id, self.role.inbound_direction())
    }

    pub(crate) const fn established_leg(&self) -> &EstablishedLeg {
        &self.established
    }

    pub(crate) const fn counters(&self) -> LegIoCounters {
        self.counters
    }

    /// Encodes before the byte-only wire can apply a fault action.
    #[cfg(test)]
    pub(crate) fn send_frame<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
        frame: Frame,
    ) -> Result<(), LegIoError> {
        self.send_retained_frame(transport, now, &frame)
    }

    /// Attempts one encoded submission while ownership of the protocol frame
    /// remains with the caller. A bounded controller queue can therefore retry
    /// the exact same DATA/control frame after transient transport pressure;
    /// endpoint counters advance only after the transport accepts it.
    pub(crate) fn send_retained_frame<T: EncodedLegTransport + ?Sized>(
        &mut self,
        transport: &mut T,
        now: SimTime,
        frame: &Frame,
    ) -> Result<(), LegIoError> {
        let lane = lane_for_record(frame.record());
        let encoded = frame.encode().map_err(LegIoError::Encode)?;
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| LegIoError::CounterOverflow)?;
        let (messages, bytes) = self.preflight_outbound(encoded_len)?;
        transport
            .send_encoded(now, self.outbound_route(), lane, encoded.to_vec())
            .map_err(LegIoError::Transport)?;
        self.counters.outbound_messages = messages;
        self.counters.outbound_bytes = bytes;
        match lane {
            WireLane::Control => {
                self.counters.outbound_control_messages += 1;
            }
            WireLane::Data => {
                self.counters.outbound_data_messages += 1;
            }
        }
        Ok(())
    }

    /// Validates transport metadata, decodes one exact byte message, validates
    /// its production lane, and only then mints this endpoint's provenance.
    pub(crate) fn receive_delivery(
        &mut self,
        delivery: EncodedDelivery,
    ) -> Result<LegBoundFrame, LegIoError> {
        let actual_route = delivery.route();
        let actual_lane = delivery.lane();
        let encoded = delivery.into_bytes();
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| LegIoError::CounterOverflow)?;
        let (messages, bytes) = self.preflight_inbound(encoded_len)?;
        self.counters.inbound_messages = messages;
        self.counters.inbound_bytes = bytes;

        let expected_route = self.inbound_route();
        if actual_route != expected_route {
            self.counters.wrong_route_rejections += 1;
            return Err(LegIoError::WrongRoute {
                expected: expected_route,
                actual: actual_route,
            });
        }

        let frame = match Frame::decode_owned_exact(Bytes::from(encoded)) {
            Ok(frame) => frame,
            Err(error) => {
                self.counters.decode_rejections += 1;
                return Err(LegIoError::Decode(error));
            }
        };
        let expected_lane = lane_for_record(frame.record());
        if actual_lane != expected_lane {
            self.counters.wrong_lane_rejections += 1;
            return Err(LegIoError::WrongLane {
                expected: expected_lane,
                actual: actual_lane,
            });
        }

        self.counters.bound_messages += 1;
        self.counters.bound_bytes = self
            .counters
            .bound_bytes
            .checked_add(encoded_len)
            .ok_or(LegIoError::CounterOverflow)?;
        Ok(self.established.bind_received_frame(frame))
    }

    fn preflight_outbound(&self, len: u64) -> Result<(u64, u64), LegIoError> {
        if len > self.limits.max_frame_bytes {
            return Err(LegIoError::FrameTooLarge {
                len,
                max: self.limits.max_frame_bytes,
            });
        }
        let messages = self
            .counters
            .outbound_messages
            .checked_add(1)
            .ok_or(LegIoError::CounterOverflow)?;
        if messages > self.limits.max_outbound_messages {
            return Err(LegIoError::OutboundMessageBudgetExceeded);
        }
        let bytes = self
            .counters
            .outbound_bytes
            .checked_add(len)
            .ok_or(LegIoError::CounterOverflow)?;
        if bytes > self.limits.max_outbound_bytes {
            return Err(LegIoError::OutboundByteBudgetExceeded);
        }
        Ok((messages, bytes))
    }

    fn preflight_inbound(&self, len: u64) -> Result<(u64, u64), LegIoError> {
        if len > self.limits.max_frame_bytes {
            return Err(LegIoError::FrameTooLarge {
                len,
                max: self.limits.max_frame_bytes,
            });
        }
        let messages = self
            .counters
            .inbound_messages
            .checked_add(1)
            .ok_or(LegIoError::CounterOverflow)?;
        if messages > self.limits.max_inbound_messages {
            return Err(LegIoError::InboundMessageBudgetExceeded);
        }
        let bytes = self
            .counters
            .inbound_bytes
            .checked_add(len)
            .ok_or(LegIoError::CounterOverflow)?;
        if bytes > self.limits.max_inbound_bytes {
            return Err(LegIoError::InboundByteBudgetExceeded);
        }
        Ok((messages, bytes))
    }
}

impl fmt::Debug for LegIo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegIo([REDACTED])")
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum LegIoError {
    #[error("leg frame encoding failed: {0}")]
    Encode(ProtocolError),
    #[error("leg frame decoding failed: {0}")]
    Decode(ProtocolError),
    #[error("leg encoded transport operation failed: {0}")]
    Transport(EncodedLegTransportError),
    #[error("leg encoded frame is too large: {len} bytes exceeds {max}")]
    FrameTooLarge { len: u64, max: u64 },
    #[error("leg outbound message budget exceeded")]
    OutboundMessageBudgetExceeded,
    #[error("leg outbound byte budget exceeded")]
    OutboundByteBudgetExceeded,
    #[error("leg inbound message budget exceeded")]
    InboundMessageBudgetExceeded,
    #[error("leg inbound byte budget exceeded")]
    InboundByteBudgetExceeded,
    #[error("leg I/O counter overflow")]
    CounterOverflow,
    #[error("encoded delivery used wrong route: expected {expected:?}, got {actual:?}")]
    WrongRoute {
        expected: WireRoute,
        actual: WireRoute,
    },
    #[error("encoded delivery used wrong lane: expected {expected:?}, got {actual:?}")]
    WrongLane {
        expected: WireLane,
        actual: WireLane,
    },
}

const fn lane_for_record(record: &Record) -> WireLane {
    match record {
        Record::Data { .. } | Record::Close { .. } => WireLane::Data,
        Record::Attach { .. }
        | Record::AttachAccepted { .. }
        | Record::AttachGenerationStatus { .. }
        | Record::Open { .. }
        | Record::OpenResult { .. }
        | Record::Ack { .. }
        | Record::Reset { .. } => WireLane::Control,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_upstream::leg::{AttachResponse, LegProvenanceError};
    use crate::owned_upstream::supervisor::SessionSupervisor;
    use crate::owned_upstream::two_leg::{WireBounds, WireCapacity, WireCapacitySpec};
    use crate::resumable::{
        AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
        AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
        Frame, LegGeneration, OwnerIdentity, ReceiveBudgetLimits, Record, ReplayBudgetLimits,
        ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionFlowId, SessionId,
        TcpWindowLimits, TlsExporterBinding, VersionRange,
    };
    use std::time::Duration;

    fn binding() -> AttachTransportBinding {
        AttachTransportBinding::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            TlsExporterBinding::new([0x42; 32]).unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
        )
    }

    fn request() -> AttachRequest {
        AttachRequest::new(
            SessionId::new([0x11; 16]).unwrap(),
            LegGeneration::new(2).unwrap(),
            AttachNonce::new([0x22; 16]).unwrap(),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
            FeatureOffer::new(0b111, 0b001).unwrap(),
        )
    }

    fn credentials() -> AttachCredentials {
        AttachCredentials::new(
            DeviceSecret::new([0x71; 32]).unwrap(),
            ResumeSecret::new([0x72; 32]).unwrap(),
        )
        .unwrap()
    }

    fn authority() -> AttachAuthority {
        AttachAuthority::new(
            request().session_id(),
            LegGeneration::new(1).unwrap(),
            credentials(),
            AttachPolicy::new(
                OwnerIdentity::new([0x31; 32]).unwrap(),
                AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
                DevicePrincipal::new([0x53; 16]).unwrap(),
                SESSION_PROTOCOL_VERSION,
                0b1111,
            )
            .unwrap(),
        )
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

    fn wire_capacity() -> WireCapacity {
        WireCapacitySpec::new(80_000, Duration::from_secs(1), 256, 128, 1)
            .unwrap()
            .with_fault_copies(1, 1)
            .unwrap()
            .derive()
            .unwrap()
    }

    fn wire() -> TwoLegWire {
        TwoLegWire::new(WireBounds::new(wire_capacity(), 64, 32_000, 4, 4).unwrap())
    }

    fn memory_transport() -> MemoryLegTransport {
        MemoryLegTransport::new(wire(), MemoryFaultScript::new(0))
    }

    fn release_due_events_through(wire: &mut MemoryLegTransport, through: SimTime) {
        while wire.advance_one_due(through).unwrap() {}
        wire.advance_idle_to(through).unwrap();
    }

    fn limits() -> LegIoLimits {
        LegIoLimits::from_wire_capacity(wire_capacity(), 1_024).unwrap()
    }

    fn endpoint(leg: LegId, role: LegEndpointRole) -> LegIo {
        LegIo::for_authenticated_transport(leg, role, binding(), limits())
    }

    fn send(
        endpoint: &mut LegIo,
        transport: &mut MemoryLegTransport,
        now: SimTime,
        frame: Frame,
    ) -> Result<(), LegIoError> {
        endpoint.send_frame(&mut transport.controller_sender(), now, frame)
    }

    fn data_frame(offset: u64, payload: &'static [u8]) -> Frame {
        Frame::try_new(
            LegGeneration::new(2).unwrap(),
            Record::Data {
                flow_id: SessionFlowId::new(7).unwrap(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(offset),
                payload: Bytes::from_static(payload),
            },
        )
        .unwrap()
    }

    #[test]
    fn frame_crosses_encode_wire_decode_and_exact_endpoint_seal() {
        let transport = binding();
        let mut client = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Client,
            transport,
            limits(),
        );
        let mut owner = LegIo::for_authenticated_transport(
            LegId::A,
            LegEndpointRole::Owner,
            transport,
            limits(),
        );
        let request = request();
        let pending = client.established_leg().begin_attach(request);
        let proof = credentials().prove(&request, &transport).unwrap();
        let mut wire = memory_transport();

        send(
            &mut client,
            &mut wire,
            SimTime::ZERO,
            request.to_attach_frame(proof),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire.try_recv_next(owner.inbound_route()).unwrap().unwrap();
        let received = owner.receive_delivery(delivery).unwrap();
        let mut pending_owner = SessionSupervisor::prepare_owner(session_config(), authority());
        let owner_bootstrap = pending_owner
            .accept(owner.established_leg(), received)
            .unwrap();
        let (owner_supervisor, owner_attached, acceptance) = owner_bootstrap.into_parts();
        assert_eq!(owner_supervisor.snapshot().session.generation().get(), 2);
        assert_eq!(owner_attached.generation().get(), 2);

        send(&mut owner, &mut wire, SimTime::ZERO, acceptance).unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire.try_recv_next(client.inbound_route()).unwrap().unwrap();
        let response = client.receive_delivery(delivery).unwrap();
        let AttachResponse::Accepted(client_attached) =
            pending.validate_response(response).unwrap()
        else {
            panic!("correlated response changed response kind");
        };
        let client_bootstrap =
            SessionSupervisor::bootstrap_client(session_config(), client_attached);
        let (client_supervisor, client_attached) = client_bootstrap.into_parts();
        assert_eq!(client_supervisor.snapshot().session.generation().get(), 2);
        assert_eq!(client_attached.generation().get(), 2);

        assert_eq!(client.counters().outbound_messages, 1);
        assert_eq!(client.counters().bound_messages, 1);
        assert_eq!(owner.counters().outbound_messages, 1);
        assert_eq!(owner.counters().bound_messages, 1);
    }

    #[test]
    fn wrong_endpoint_route_and_wire_lane_fail_closed() {
        let mut client_a = endpoint(LegId::A, LegEndpointRole::Client);
        let mut owner_b = endpoint(LegId::B, LegEndpointRole::Owner);
        let request = request();
        let proof = credentials().prove(&request, &binding()).unwrap();
        let mut wire = memory_transport();

        send(
            &mut client_a,
            &mut wire,
            SimTime::ZERO,
            request.to_attach_frame(proof),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire
            .try_recv_next(WireRoute::new(LegId::A, WireDirection::ClientToOwner))
            .unwrap()
            .unwrap();
        assert!(matches!(
            owner_b.receive_delivery(delivery),
            Err(LegIoError::WrongRoute {
                expected,
                actual,
            }) if expected == WireRoute::new(LegId::B, WireDirection::ClientToOwner)
                && actual == WireRoute::new(LegId::A, WireDirection::ClientToOwner)
        ));
        assert_eq!(owner_b.counters().wrong_route_rejections, 1);
        assert_eq!(owner_b.counters().bound_messages, 0);

        let mut owner_a = endpoint(LegId::A, LegEndpointRole::Owner);
        let encoded = data_frame(0, b"wrong-lane").encode().unwrap();
        wire.inject_encoded_test_message(
            SimTime::ZERO,
            owner_a.inbound_route(),
            WireLane::Control,
            encoded.to_vec(),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let delivery = wire
            .try_recv_next(owner_a.inbound_route())
            .unwrap()
            .unwrap();
        assert!(matches!(
            owner_a.receive_delivery(delivery),
            Err(LegIoError::WrongLane {
                expected: WireLane::Data,
                actual: WireLane::Control,
            })
        ));
        assert_eq!(owner_a.counters().wrong_lane_rejections, 1);
        assert_eq!(owner_a.counters().bound_messages, 0);
    }

    #[test]
    fn truncated_and_corrupt_bytes_are_rejected_before_exact_seal_binding() {
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let route = owner.inbound_route();
        let frame = data_frame(0, b"malformed-wire");
        let mut truncated = frame.encode().unwrap().to_vec();
        truncated.pop();
        let mut corrupt = frame.encode().unwrap().to_vec();
        corrupt[0] ^= 0xff;
        let mut wire = memory_transport();

        wire.inject_encoded_test_message(SimTime::ZERO, route, WireLane::Data, truncated)
            .unwrap();
        wire.inject_encoded_test_message(SimTime::ZERO, route, WireLane::Data, corrupt)
            .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);

        assert!(matches!(
            owner.receive_delivery(wire.try_recv_next(route).unwrap().unwrap()),
            Err(LegIoError::Decode(
                crate::resumable::ProtocolError::Truncated { .. }
            ))
        ));
        assert!(matches!(
            owner.receive_delivery(wire.try_recv_next(route).unwrap().unwrap()),
            Err(LegIoError::Decode(
                crate::resumable::ProtocolError::InvalidMagic
            ))
        ));
        assert_eq!(owner.counters().inbound_messages, 2);
        assert_eq!(owner.counters().decode_rejections, 2);
        assert_eq!(owner.counters().bound_messages, 0);
        assert_eq!(owner.counters().bound_bytes, 0);
    }

    #[test]
    fn identical_binding_on_legs_a_and_b_does_not_merge_exact_local_seals() {
        let mut client_a = endpoint(LegId::A, LegEndpointRole::Client);
        let mut owner_a = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut client_b = endpoint(LegId::B, LegEndpointRole::Client);
        let mut owner_b = endpoint(LegId::B, LegEndpointRole::Owner);
        let request = request();
        let pending = client_a.established_leg().begin_attach(request);
        let proof = credentials().prove(&request, &binding()).unwrap();
        let mut wire = memory_transport();

        send(
            &mut client_a,
            &mut wire,
            SimTime::ZERO,
            request.to_attach_frame(proof),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let attach = owner_a
            .receive_delivery(
                wire.try_recv_next(owner_a.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let mut pending_owner = SessionSupervisor::prepare_owner(session_config(), authority());
        let owner_bootstrap = pending_owner
            .accept(owner_a.established_leg(), attach)
            .unwrap();
        let (_owner_supervisor, _owner_attached, acceptance) = owner_bootstrap.into_parts();
        send(&mut owner_a, &mut wire, SimTime::ZERO, acceptance).unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let response = client_a
            .receive_delivery(
                wire.try_recv_next(client_a.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let AttachResponse::Accepted(attached_a) = pending.validate_response(response).unwrap()
        else {
            panic!("correlated response changed response kind");
        };

        send(
            &mut owner_b,
            &mut wire,
            SimTime::ZERO,
            data_frame(0, b"leg-b-only"),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        let from_b = client_b
            .receive_delivery(
                wire.try_recv_next(client_b.inbound_route())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            attached_a.accept_frame(from_b),
            Err(LegProvenanceError::WrongLeg)
        ));
    }

    #[test]
    fn blackout_drop_and_duplicate_apply_before_decode_only_to_encoded_bytes() {
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut client = endpoint(LegId::A, LegEndpointRole::Client);
        let route = owner.outbound_route();
        let mut raw_wire = wire();
        raw_wire
            .add_blackout(route, SimTime::from_nanos(10), SimTime::from_nanos(20))
            .unwrap();
        let mut faults = MemoryFaultScript::new(2);
        faults
            .insert(0, route, WireLane::Data, FaultAction::Drop)
            .unwrap();
        faults
            .insert(1, route, WireLane::Data, FaultAction::Duplicate)
            .unwrap();
        let mut wire = MemoryLegTransport::new(raw_wire, faults);

        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(1),
            data_frame(0, b"drop"),
        )
        .unwrap();
        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(2),
            data_frame(4, b"duplicate"),
        )
        .unwrap();
        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(10),
            data_frame(13, b"blackout"),
        )
        .unwrap();
        send(
            &mut owner,
            &mut wire,
            SimTime::from_nanos(20),
            data_frame(21, b"after"),
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::from_nanos(20));

        let mut received = 0;
        while let Some(delivery) = wire.try_recv_next(route).unwrap() {
            client.receive_delivery(delivery).unwrap();
            received += 1;
        }
        assert_eq!(received, 3);
        assert_eq!(client.counters().bound_messages, 3);
        assert_eq!(client.counters().decode_rejections, 0);

        let wire_counters = wire.wire_counters();
        assert_eq!(wire_counters.submitted_messages, 4);
        assert_eq!(wire_counters.physical_messages, 3);
        assert_eq!(wire_counters.delivered_messages, 3);
        assert_eq!(wire_counters.dropped_messages, 2);
        assert_eq!(wire_counters.blackout_drops, 1);
        assert_eq!(wire_counters.duplicate_copies, 1);
    }

    #[test]
    fn finite_endpoint_budget_rejects_without_submitting_an_extra_message() {
        let limits = LegIoLimits::new(1_024, 1, 4_096, 1, 4_096).unwrap();
        let mut owner =
            LegIo::for_authenticated_transport(LegId::A, LegEndpointRole::Owner, binding(), limits);
        let mut wire = memory_transport();
        send(
            &mut owner,
            &mut wire,
            SimTime::ZERO,
            data_frame(0, b"first"),
        )
        .unwrap();
        assert_eq!(
            send(
                &mut owner,
                &mut wire,
                SimTime::ZERO,
                data_frame(5, b"second"),
            ),
            Err(LegIoError::OutboundMessageBudgetExceeded)
        );
        assert_eq!(owner.counters().outbound_messages, 1);
        assert_eq!(wire.wire_counters().submitted_messages, 1);
    }

    #[test]
    fn preconfigured_fault_script_mismatch_fails_before_wire_submission() {
        let mut owner = endpoint(LegId::A, LegEndpointRole::Owner);
        let mut faults = MemoryFaultScript::new(1);
        faults
            .insert(
                0,
                WireRoute::new(LegId::A, WireDirection::ClientToOwner),
                WireLane::Data,
                FaultAction::Drop,
            )
            .unwrap();
        let mut wire = MemoryLegTransport::new(wire(), faults);

        assert!(matches!(
            send(
                &mut owner,
                &mut wire,
                SimTime::ZERO,
                data_frame(0, b"not-the-scripted-route"),
            ),
            Err(LegIoError::Transport(
                EncodedLegTransportError::FaultScriptMismatch { ordinal: 0, .. }
            ))
        ));
        assert_eq!(owner.counters().outbound_messages, 0);
        assert_eq!(wire.wire_counters().submitted_messages, 0);
        assert_eq!(wire.remaining_fault_directives(), 1);
    }

    // Ambiguous inference fails to compile if the endpoint authority ever
    // becomes Clone.  The exact transport seal must remain single-owner.
    trait AmbiguousIfClone<Marker> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    const _: fn() = || {
        let _ = <LegIo as AmbiguousIfClone<_>>::marker;
    };

    #[test]
    fn endpoint_debug_redacts_authenticated_transport_authority() {
        let endpoint = endpoint(LegId::A, LegEndpointRole::Client);
        assert_eq!(format!("{endpoint:?}"), "LegIo([REDACTED])");
    }
}
