//! Borrow-only inventory of original transport-independent Session payloads.
//! Owns no resource or ledger; entry methods inspect existing frames/events.
//! Capacities are actual retained backing; Bytes lengths are logical views,
//! not a unique-allocation/peak claim. Copy capabilities count value positions,
//! not physical authorities. No allocation, recursion or DATA-path operation.
//! Tests live in payload_inventory/tests/mod.rs.

use super::effect_storage::SessionEffectBatch;
use super::{Frame, LegControlFrame, Record, SessionEffect, SessionEvent, TcpDataSegment};
use crate::shared::TargetAddr;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PayloadAuditFailure {
    CountOverflow,
    Layout,
    InvalidFrame,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SessionPayloadInventory {
    pub(crate) events: usize,
    pub(crate) session_frames: usize,
    pub(crate) session_encoded_bytes: usize,
    pub(crate) control_frames: usize,
    pub(crate) control_encoded_bytes: usize,
    pub(crate) byte_views: usize,
    pub(crate) logical_bytes: usize,
    pub(crate) targets: usize,
    pub(crate) domain_bytes: usize,
    pub(crate) domain_capacity: usize,
    pub(crate) capability_values: usize,
    pub(crate) effect_batches: usize,
    pub(crate) effect_vectors: usize,
    pub(crate) effects: usize,
    pub(crate) effect_capacity: usize,
    pub(crate) effect_array_bytes: usize,
    pub(crate) overflow_effects: usize,
    pub(crate) mutated_batches: usize,
    pub(crate) segment_values: usize,
    pub(crate) segment_capacity: usize,
    pub(crate) segment_array_bytes: usize,
    pub(crate) segment_backing_charges: usize,
    pub(crate) receipt_values: usize,
}

impl SessionPayloadInventory {
    pub(crate) const fn empty() -> Self {
        Self {
            events: 0,
            session_frames: 0,
            session_encoded_bytes: 0,
            control_frames: 0,
            control_encoded_bytes: 0,
            byte_views: 0,
            logical_bytes: 0,
            targets: 0,
            domain_bytes: 0,
            domain_capacity: 0,
            capability_values: 0,
            effect_batches: 0,
            effect_vectors: 0,
            effects: 0,
            effect_capacity: 0,
            effect_array_bytes: 0,
            overflow_effects: 0,
            mutated_batches: 0,
            segment_values: 0,
            segment_capacity: 0,
            segment_array_bytes: 0,
            segment_backing_charges: 0,
            receipt_values: 0,
        }
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.events == 0
            && self.session_frames == 0
            && self.session_encoded_bytes == 0
            && self.control_frames == 0
            && self.control_encoded_bytes == 0
            && self.byte_views == 0
            && self.logical_bytes == 0
            && self.targets == 0
            && self.domain_bytes == 0
            && self.domain_capacity == 0
            && self.capability_values == 0
            && self.effect_batches == 0
            && self.effect_vectors == 0
            && self.effects == 0
            && self.effect_capacity == 0
            && self.effect_array_bytes == 0
            && self.overflow_effects == 0
            && self.mutated_batches == 0
            && self.segment_values == 0
            && self.segment_capacity == 0
            && self.segment_array_bytes == 0
            && self.segment_backing_charges == 0
            && self.receipt_values == 0
    }

    pub(crate) fn checked_add(self, other: Self) -> Result<Self, PayloadAuditFailure> {
        let add = |a: usize, b: usize| a.checked_add(b).ok_or(PayloadAuditFailure::CountOverflow);
        Ok(Self {
            events: add(self.events, other.events)?,
            session_frames: add(self.session_frames, other.session_frames)?,
            session_encoded_bytes: add(self.session_encoded_bytes, other.session_encoded_bytes)?,
            control_frames: add(self.control_frames, other.control_frames)?,
            control_encoded_bytes: add(self.control_encoded_bytes, other.control_encoded_bytes)?,
            byte_views: add(self.byte_views, other.byte_views)?,
            logical_bytes: add(self.logical_bytes, other.logical_bytes)?,
            targets: add(self.targets, other.targets)?,
            domain_bytes: add(self.domain_bytes, other.domain_bytes)?,
            domain_capacity: add(self.domain_capacity, other.domain_capacity)?,
            capability_values: add(self.capability_values, other.capability_values)?,
            effect_batches: add(self.effect_batches, other.effect_batches)?,
            effect_vectors: add(self.effect_vectors, other.effect_vectors)?,
            effects: add(self.effects, other.effects)?,
            effect_capacity: add(self.effect_capacity, other.effect_capacity)?,
            effect_array_bytes: add(self.effect_array_bytes, other.effect_array_bytes)?,
            overflow_effects: add(self.overflow_effects, other.overflow_effects)?,
            mutated_batches: add(self.mutated_batches, other.mutated_batches)?,
            segment_values: add(self.segment_values, other.segment_values)?,
            segment_capacity: add(self.segment_capacity, other.segment_capacity)?,
            segment_array_bytes: add(self.segment_array_bytes, other.segment_array_bytes)?,
            segment_backing_charges: add(
                self.segment_backing_charges,
                other.segment_backing_charges,
            )?,
            receipt_values: add(self.receipt_values, other.receipt_values)?,
        })
    }

    pub(crate) fn frame(frame: &Frame) -> Result<Self, PayloadAuditFailure> {
        let mut inventory = Self {
            session_frames: 1,
            session_encoded_bytes: frame
                .encoded_len_checked()
                .map_err(|_| PayloadAuditFailure::InvalidFrame)?,
            ..Self::empty()
        };
        let payload = match frame.record() {
            Record::Open { target, .. } => Self::target(target)?,
            Record::Data { payload, .. } => Self::bytes(payload.len()),
            Record::Attach { .. }
            | Record::AttachAccepted { .. }
            | Record::AttachGenerationStatus { .. }
            | Record::OpenResult { .. }
            | Record::Ack { .. }
            | Record::Close { .. }
            | Record::Reset { .. } => Self::empty(),
        };
        inventory = inventory.checked_add(payload)?;
        Ok(inventory)
    }

    pub(crate) fn leg_control(frame: &LegControlFrame) -> Result<Self, PayloadAuditFailure> {
        Ok(Self {
            control_frames: 1,
            control_encoded_bytes: frame
                .encoded_len_checked()
                .map_err(|_| PayloadAuditFailure::InvalidFrame)?,
            ..Self::empty()
        })
    }

    pub(crate) fn event(event: &SessionEvent) -> Result<Self, PayloadAuditFailure> {
        let value = match event {
            SessionEvent::LocalOpen { leg, target } => {
                require_copy(leg);
                Self::target(target)?
            }
            SessionEvent::LocalData { flow, payload } => {
                require_copy(flow);
                Self::bytes(payload.len())
            }
            SessionEvent::PeerFrame { leg, frame } => {
                require_copy(leg);
                Self::frame(frame)?
            }
            SessionEvent::ReplacementAttached { leg }
            | SessionEvent::LegLost { leg }
            | SessionEvent::ResumeGraceExpired { leg } => {
                require_copy(leg);
                Self::empty()
            }
            SessionEvent::ReplacementCaughtUp { catch_up } => {
                require_copy(catch_up);
                Self::empty()
            }
            SessionEvent::LocalClose { flow } | SessionEvent::LocalReset { flow, .. } => {
                require_copy(flow);
                Self::empty()
            }
            SessionEvent::PeerOpenResolved { request, .. } => {
                require_copy(request);
                Self::empty()
            }
            SessionEvent::SinkAccepted { offer, .. } | SessionEvent::SinkAbandoned { offer } => {
                require_copy(offer);
                Self::empty()
            }
            SessionEvent::SinkHalfClosed { completion }
            | SessionEvent::SinkHalfCloseFailed { completion } => {
                require_copy(completion);
                Self::empty()
            }
            SessionEvent::TerminalGraceExpired { terminal } => {
                require_copy(terminal);
                Self::empty()
            }
        };
        Self {
            events: 1,
            capability_values: 1,
            ..Self::empty()
        }
        .checked_add(value)
    }

    pub(crate) fn batch(batch: &SessionEffectBatch) -> Result<Self, PayloadAuditFailure> {
        let mut inventory = Self {
            effect_batches: 1,
            effect_capacity: batch.capacity(),
            effect_array_bytes: Layout::array::<SessionEffect>(batch.capacity())
                .map_err(|_| PayloadAuditFailure::Layout)?
                .size(),
            overflow_effects: usize::from(batch.has_overflow()),
            mutated_batches: usize::from(batch.mutation_started()),
            ..Self::empty()
        };
        for effect in batch.iter() {
            inventory = inventory.checked_add(Self::effect(effect)?)?;
        }
        Ok(inventory)
    }

    /// The Vec reference is intentional: retained capacity is part of this audit.
    pub(crate) fn effect_vec(values: &Vec<SessionEffect>) -> Result<Self, PayloadAuditFailure> {
        let mut inventory = Self {
            effect_vectors: 1,
            effect_capacity: values.capacity(),
            effect_array_bytes: Layout::array::<SessionEffect>(values.capacity())
                .map_err(|_| PayloadAuditFailure::Layout)?
                .size(),
            ..Self::empty()
        };
        for effect in values {
            inventory = inventory.checked_add(Self::effect(effect)?)?;
        }
        Ok(inventory)
    }

    fn effect(effect: &SessionEffect) -> Result<Self, PayloadAuditFailure> {
        let payload = match effect {
            SessionEffect::Transmit(frame) => Self::frame(frame)?,
            SessionEffect::ReplayStored { receipt } => Self {
                receipt_values: 1,
                capability_values: 1,
                ..Self::bytes(receipt.payload().len())
            },
            SessionEffect::ReplayAcknowledged { receipt: _ } => Self {
                receipt_values: 1,
                capability_values: 1,
                ..Self::empty()
            },
            SessionEffect::LocalFlowOpened { flow } => {
                require_copy(flow);
                Self {
                    capability_values: 1,
                    ..Self::empty()
                }
            }
            SessionEffect::PeerOpenRequested {
                request,
                flow,
                target,
            } => {
                require_copy(request);
                require_copy(flow);
                Self {
                    capability_values: 2,
                    ..Self::target(target)?
                }
            }
            SessionEffect::OfferToSink { offer, segments } => {
                require_copy(offer);
                let mut value = Self {
                    capability_values: 1,
                    segment_values: segments.len(),
                    segment_capacity: segments.capacity(),
                    segment_array_bytes: Layout::array::<TcpDataSegment>(segments.capacity())
                        .map_err(|_| PayloadAuditFailure::Layout)?
                        .size(),
                    ..Self::empty()
                };
                for segment in segments {
                    value = value.checked_add(Self {
                        segment_backing_charges: segment.backing_bytes(),
                        ..Self::bytes(segment.len())
                    })?;
                }
                value
            }
            SessionEffect::HalfCloseSink { completion } => {
                require_copy(completion);
                Self {
                    capability_values: 1,
                    ..Self::empty()
                }
            }
            SessionEffect::FlowFinished { terminal, .. } => {
                require_copy(terminal);
                Self {
                    capability_values: 1,
                    ..Self::empty()
                }
            }
            SessionEffect::LegActivated { .. }
            | SessionEffect::ResumeGraceStarted { .. }
            | SessionEffect::SessionExpired
            | SessionEffect::LocalOpenResolved { .. }
            | SessionEffect::PeerReset { .. } => Self::empty(),
        };
        Self {
            effects: 1,
            ..Self::empty()
        }
        .checked_add(payload)
    }

    fn bytes(len: usize) -> Self {
        Self {
            byte_views: 1,
            logical_bytes: len,
            ..Self::empty()
        }
    }

    fn target(target: &TargetAddr) -> Result<Self, PayloadAuditFailure> {
        let mut inventory = Self {
            targets: 1,
            ..Self::empty()
        };
        match target {
            TargetAddr::IpPort(_) => {}
            TargetAddr::DomainPort { host, port: _ } => {
                inventory.domain_bytes = host.len();
                inventory.domain_capacity = Layout::array::<u8>(host.capacity())
                    .map_err(|_| PayloadAuditFailure::Layout)?
                    .size();
            }
        }
        Ok(inventory)
    }
}

fn require_copy<T: Copy>(_: &T) {}

#[cfg(test)]
mod tests;
