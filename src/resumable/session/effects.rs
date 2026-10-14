//! Ordinary session-event dispatch and its complete effect-producing operations.
//! Owns protocol validation/commit/effect order; parent retains state, LocalOpen
//! preparation and replacement/recovery transactions. No I/O or lifecycle owner.
//! Entry: SessionModel::reduce; characterization lives in the parent tests.

use super::*;
use crate::resumable::effect_storage::SessionEffectBatch;

#[cfg(test)]
mod tests;

/// Fully prepared publication at the original Session owner. The same segment
/// allocation is moved to the output, never reconstructed after TCP mutation.
struct PreparedSinkOffer {
    offer: SinkOffer,
    segments: Vec<TcpDataSegment>,
}
impl PreparedSinkOffer {
    fn new(
        session_id: SessionId,
        direction: Direction,
        flow_id: SessionFlowId,
        next_id: Option<u64>,
        segments: Vec<TcpDataSegment>,
    ) -> Result<Option<Self>, SessionError> {
        let Some(first) = segments.first() else {
            return Ok(None);
        };
        let len = segments
            .iter()
            .try_fold(0usize, |n, s| n.checked_add(s.len()))
            .ok_or(SessionError::InvariantViolation(
                "sink offer length overflow",
            ))?;
        let offer = SinkOffer {
            session_id,
            id: next_id.ok_or(SessionError::SinkOfferIdExhausted)?,
            flow_id,
            direction,
            offset: first.offset(),
            len,
        };
        Ok(Some(Self { offer, segments }))
    }
}

impl SessionModel {
    pub fn reduce(&mut self, event: SessionEvent) -> Result<Vec<SessionEffect>, SessionError> {
        self.reduce_owned(event, SessionEffectBatch::empty())
    }

    fn reduce_owned(
        &mut self,
        event: SessionEvent,
        mut effects: SessionEffectBatch,
    ) -> Result<Vec<SessionEffect>, SessionError> {
        self.require_effects_live()?;
        if let Err(error) = self.reduce_into(event, &mut effects) {
            if !effects.is_empty() || effects.mutation_started() {
                self.failed_effects = Some(effects);
            }
            return Err(error);
        }
        // Every append propagates its first error; only a complete normal batch
        // can reach this allocation-preserving public compatibility handoff.
        Ok(effects.into_parts().0)
    }

    pub(crate) fn reduce_into(
        &mut self,
        event: SessionEvent,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.require_effects_live()?;
        if !effects.is_empty() || effects.mutation_started() {
            return Err(SessionError::EffectStorageOverflow);
        }
        match event {
            SessionEvent::ReplacementAttached { leg } => self.attach_replacement(leg, effects),
            SessionEvent::ReplacementCaughtUp { catch_up } => {
                self.attach_catch_up(catch_up, effects)
            }
            SessionEvent::LegLost { leg } => self.lose_leg(leg, effects),
            SessionEvent::ResumeGraceExpired { leg } => self.expire_resume_grace(leg, effects),
            SessionEvent::LocalOpen { leg, target } => self.local_open(leg, target, effects),
            SessionEvent::LocalData { flow, payload } => self.local_data(flow, payload, effects),
            SessionEvent::LocalClose { flow } => self.local_close(flow, effects),
            SessionEvent::LocalReset { flow, reason } => self.local_reset(flow, reason, effects),
            SessionEvent::PeerOpenResolved { request, result } => {
                self.peer_open_resolved(request, result, effects)
            }
            SessionEvent::PeerFrame { leg, frame } => self.peer_frame(leg, frame, effects),
            SessionEvent::SinkAccepted { offer, bytes } => {
                self.sink_accepted(offer, bytes, effects)
            }
            SessionEvent::SinkAbandoned { offer } => self.sink_abandoned(offer, effects),
            SessionEvent::SinkHalfClosed { completion } => {
                self.sink_half_closed(completion, effects)
            }
            SessionEvent::SinkHalfCloseFailed { completion } => {
                self.sink_half_close_failed(completion, effects)
            }
            SessionEvent::TerminalGraceExpired { terminal } => {
                self.expire_terminal_grace(terminal, effects)
            }
        }?;
        Ok(())
    }

    fn lose_leg(
        &mut self,
        leg: CommittedLeg,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.validate_current_capability(&leg)?;
        if self.phase == SessionPhase::Legless {
            return Ok(());
        }
        effects.prepare_ordinary()?;
        effects.mark_mutation();
        self.phase = SessionPhase::Legless;
        {
            effects.push(SessionEffect::ResumeGraceStarted {
                generation: leg.generation(),
            })?;
            Ok(())
        }
    }

    fn local_data(
        &mut self,
        local_flow: LocalFlow,
        payload: Bytes,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let flow_id = self.validate_local_flow(local_flow)?;
        self.precheck_flow_live(flow_id)?;
        self.precheck_local_source_open(flow_id)?;
        let direction = self.role.local_send_direction();
        self.precheck_replay_budget(direction, payload.len(), 1)?;

        let (append, owned_payload) = {
            let flow = self.flow_mut(flow_id)?;
            let offset = flow.send.next_sent();
            let prepared = flow
                .send
                .prepare_append(offset, payload)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            effects.prepare_ordinary()?;
            effects.mark_mutation();
            let append = prepared.commit();
            let owned_payload = flow
                .send
                .replay_view()
                .last()
                .filter(|segment| {
                    segment.offset() == append.offset()
                        && segment.end_offset() == append.end_offset()
                })
                .map(|segment| segment.payload().clone())
                .ok_or(SessionError::InvariantViolation(
                    "new sender ownership has no replay view",
                ))?;
            (append, owned_payload)
        };
        self.add_replay_usage(
            direction,
            append.retained_bytes_added(),
            append.segments_added(),
        )?;

        let receipt = ReplayStored {
            local_flow,
            direction,
            start: append.offset(),
            end: append.end_offset(),
            payload: owned_payload.clone(),
        };
        effects.push(SessionEffect::ReplayStored { receipt })?;
        self.transmit_if_active(
            Record::Data {
                flow_id,
                direction,
                offset: append.offset(),
                payload: owned_payload,
            },
            effects,
        )?;
        Ok(())
    }

    fn local_close(
        &mut self,
        local_flow: LocalFlow,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let flow_id = self.validate_local_flow(local_flow)?;
        self.precheck_flow_live(flow_id)?;
        self.precheck_local_source_open(flow_id)?;
        let direction = self.role.local_send_direction();
        let active = self.phase == SessionPhase::Active;
        let close = {
            let flow = self.flow_mut(flow_id)?;
            let final_offset = flow.send.next_sent();
            let prepared = flow
                .send
                .prepare_close(final_offset)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            if active && !prepared.outcome().final_accepted() {
                effects.prepare_ordinary()?;
            }
            effects.mark_mutation();
            prepared.commit()
        };
        if close.final_accepted() {
            return Ok(());
        }
        self.transmit_if_active(
            Record::Close {
                flow_id,
                direction,
                final_offset: close.final_offset(),
            },
            effects,
        )
    }

    fn local_reset(
        &mut self,
        local_flow: LocalFlow,
        reason: ResetReason,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let flow_id = self.validate_local_flow(local_flow)?;
        self.precheck_flow_live(flow_id)?;
        self.precheck_flow_finish(flow_id)?;
        let control = Record::Reset { flow_id, reason };
        self.transmit_if_active(control.clone(), effects)?;
        self.finish_flow(
            flow_id,
            FlowFinishReason::LocalReset(reason),
            Some(control),
            effects,
        )?;
        Ok(())
    }

    fn peer_open_resolved(
        &mut self,
        request: PeerOpenRequest,
        result: OpenResultCode,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let flow_id = self.validate_peer_open_request(request)?;
        if self.role != SessionRole::Owner {
            return Err(SessionError::RoleCannotResolvePeerOpen(self.role));
        }
        self.precheck_flow_live(flow_id)?;
        let activates_sink = if result != OpenResultCode::Opened {
            self.precheck_terminal_capacity()?;
            false
        } else {
            self.precheck_sink_activation_after_open(flow_id)?
        };
        let finish_check =
            (result != OpenResultCode::Opened).then(|| self.precheck_flow_finish(flow_id));
        let needs_effect = self.phase == SessionPhase::Active
            || activates_sink
            || result != OpenResultCode::Opened;
        let sink_offer = {
            let flow = self
                .flow_storage
                .get_mut(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            if flow.origin != FlowOrigin::Peer {
                return Err(SessionError::FlowOriginMismatch { flow_id });
            }
            if let Some(recorded) = flow.peer_open_result {
                if recorded != result {
                    return Err(SessionError::ConflictingOpenResult {
                        flow_id,
                        recorded,
                        attempted: result,
                    });
                }
            }
            if let Some(check) = finish_check {
                check?;
            }
            let segments = if result == OpenResultCode::Opened && flow.pending_sink_offer.is_none()
            {
                flow.receive
                    .prepare_contiguous_offer(self.config.max_sink_offer_bytes())
                    .map_err(|source| SessionError::TcpOwnership { flow_id, source })?
            } else {
                Vec::new()
            };
            let sink_offer = PreparedSinkOffer::new(
                self.session_id,
                self.role.peer_receive_direction(),
                flow_id,
                self.next_sink_offer_id,
                segments,
            )?;
            if needs_effect {
                effects.prepare_ordinary()?;
            }
            if flow.peer_open_result.is_none() {
                effects.mark_mutation();
                flow.peer_open_result = Some(result);
            }
            sink_offer
        };
        let control = Record::OpenResult { flow_id, result };
        self.transmit_if_active(control.clone(), effects)?;
        if result == OpenResultCode::Opened {
            self.activate_sink_after_open(flow_id, sink_offer, effects)?;
        } else {
            self.finish_flow(
                flow_id,
                FlowFinishReason::OpenFailed(result),
                Some(control),
                effects,
            )?;
        }
        Ok(())
    }

    fn peer_frame(
        &mut self,
        leg: CommittedLeg,
        frame: Frame,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.validate_current_capability(&leg)?;
        if self.phase != SessionPhase::Active {
            return Err(SessionError::PeerFrameWhileLegless);
        }
        if frame.leg_generation() != self.current_leg.generation() {
            return Err(SessionError::FrameGenerationMismatch {
                current: self.current_leg.generation().get(),
                actual: frame.leg_generation().get(),
            });
        }
        validate_record(frame.record())
            .map_err(|source| SessionError::InvalidProtocolRecord { source })?;
        match frame.record().clone() {
            Record::Open { flow_id, target } => self.receive_open(flow_id, target, effects),
            Record::OpenResult { flow_id, result } => {
                self.receive_open_result(flow_id, result, effects)
            }
            Record::Data {
                flow_id,
                direction,
                offset,
                payload,
            } => self.receive_data(flow_id, direction, offset, payload, effects),
            Record::Ack {
                flow_id,
                direction,
                next_accepted,
                final_accepted,
            } => self.receive_ack(flow_id, direction, next_accepted, final_accepted, effects),
            Record::Close {
                flow_id,
                direction,
                final_offset,
            } => self.receive_close(flow_id, direction, final_offset, effects),
            Record::Reset { flow_id, reason } => self.receive_reset(flow_id, reason, effects),
            Record::Attach { .. }
            | Record::AttachAccepted { .. }
            | Record::AttachGenerationStatus { .. } => Err(SessionError::UnexpectedAttachRecord),
        }
    }

    fn receive_open_result(
        &mut self,
        flow_id: SessionFlowId,
        result: OpenResultCode,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        if self.role != SessionRole::Client {
            return Err(SessionError::UnexpectedPeerOpenResult(self.role));
        }
        if let Some(tombstone) = self.flow_storage.get_terminal(&flow_id) {
            return self.replay_terminal_control(tombstone, effects);
        }
        self.precheck_flow_live(flow_id)?;
        if result != OpenResultCode::Opened {
            self.precheck_terminal_capacity()?;
        } else {
            self.precheck_sink_activation_after_open(flow_id)?;
        }
        let finish_check =
            (result != OpenResultCode::Opened).then(|| self.precheck_flow_finish(flow_id));
        let sink_offer = {
            let flow = self
                .flow_storage
                .get_mut(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            if flow.origin != FlowOrigin::Local {
                return Err(SessionError::FlowOriginMismatch { flow_id });
            }
            if let Some(recorded) = flow.local_open_result {
                if recorded != result {
                    return Err(SessionError::ConflictingOpenResult {
                        flow_id,
                        recorded,
                        attempted: result,
                    });
                }
                return Ok(());
            }
            if let Some(check) = finish_check {
                check?;
            }
            let segments = if result == OpenResultCode::Opened && flow.pending_sink_offer.is_none()
            {
                flow.receive
                    .prepare_contiguous_offer(self.config.max_sink_offer_bytes())
                    .map_err(|source| SessionError::TcpOwnership { flow_id, source })?
            } else {
                Vec::new()
            };
            let sink_offer = PreparedSinkOffer::new(
                self.session_id,
                self.role.peer_receive_direction(),
                flow_id,
                self.next_sink_offer_id,
                segments,
            )?;
            effects.prepare_ordinary()?;
            effects.mark_mutation();
            flow.local_open_result = Some(result);
            flow.local_open_pending = false;
            sink_offer
        };
        effects.push(SessionEffect::LocalOpenResolved { flow_id, result })?;
        if result == OpenResultCode::Opened {
            self.activate_sink_after_open(flow_id, sink_offer, effects)?;
        } else {
            self.finish_flow(flow_id, FlowFinishReason::OpenFailed(result), None, effects)?;
        }
        Ok(())
    }

    fn receive_data(
        &mut self,
        flow_id: SessionFlowId,
        direction: Direction,
        offset: ByteOffset,
        payload: Bytes,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.require_peer_receive_direction(direction)?;
        if let Some(tombstone) = self.flow_storage.get_terminal(&flow_id) {
            return self.replay_terminal_control(tombstone, effects);
        }
        self.precheck_flow_live(flow_id)?;
        let ((expected_bytes, expected_ranges), may_offer) = {
            let flow = self
                .flow_storage
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            (
                flow.receive
                    .preview_receive_delta(offset, &payload)
                    .map_err(|source| SessionError::TcpOwnership { flow_id, source })?,
                flow.sink_is_open() && flow.pending_sink_offer.is_none(),
            )
        };
        self.precheck_receive_budget(expected_bytes, expected_ranges)?;
        if may_offer && self.next_sink_offer_id.is_none() {
            return Err(SessionError::SinkOfferIdExhausted);
        }
        let next_usage = ReplayBudgetUsage {
            bytes: self.receive_usage.bytes.checked_add(expected_bytes).ok_or(
                SessionError::InvariantViolation("global receive byte accounting overflow"),
            )?,
            segments: self
                .receive_usage
                .segments
                .checked_add(expected_ranges)
                .ok_or(SessionError::InvariantViolation(
                    "global receive range accounting overflow",
                ))?,
        };
        let (committed, sink_offer) = {
            let flow = self
                .flow_storage
                .get_mut(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            let should_offer = flow.pending_sink_offer.is_none() && flow.sink_is_open();
            let reservation = flow
                .receive
                .preview_receive(offset, &payload)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            let prepared = flow
                .receive
                .prepare_receive_commit(reservation, offset, payload)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            let segments = if should_offer {
                prepared
                    .prepare_offer(self.config.max_sink_offer_bytes())
                    .map_err(|source| SessionError::TcpOwnership { flow_id, source })?
            } else {
                Vec::new()
            };
            let sink_offer = PreparedSinkOffer::new(
                self.session_id,
                self.role.peer_receive_direction(),
                flow_id,
                self.next_sink_offer_id,
                segments,
            )?;
            if sink_offer.is_some() {
                effects.prepare_ordinary()?;
            }
            effects.mark_mutation();
            (prepared.commit(), sink_offer)
        };
        self.receive_usage = next_usage;
        self.publish_sink_offer(sink_offer, effects)?;
        committed.finish();
        Ok(())
    }

    fn receive_ack(
        &mut self,
        flow_id: SessionFlowId,
        direction: Direction,
        next_accepted: ByteOffset,
        final_accepted: bool,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.require_local_send_direction(direction)?;
        if let Some(tombstone) = self.flow_storage.get_terminal(&flow_id) {
            return self.replay_terminal_control(tombstone, effects);
        }
        self.precheck_flow_live(flow_id)?;
        let may_finish = final_accepted
            && self
                .flow_storage
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?
                .receive_final_accepted;
        if may_finish {
            self.precheck_terminal_capacity()?;
        }
        let local_flow = self
            .flow_storage
            .get(&flow_id)
            .and_then(|flow| flow.local_flow)
            .ok_or(SessionError::InvariantViolation(
                "live flow has no local ACK authority",
            ))?;
        // Finish checks are pure and deferred until after the original TCP and
        // global ACK checks. Subtracting the same exact ACK delta from flow and
        // aggregate ownership preserves every terminal-release inequality.
        let finishing = self.flow_storage.get(&flow_id).is_some_and(|flow| {
            (flow.send.final_accepted() || final_accepted) && flow.receive_final_accepted
        });
        let finish_check = finishing.then(|| self.precheck_flow_finish(flow_id));
        let usage = self.replay_usage(direction);
        let acknowledged = {
            let flow = self.flow_mut(flow_id)?;
            let prepared = flow
                .send
                .prepare_acknowledge(next_accepted, final_accepted)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            let delta = prepared.outcome();
            if usage.bytes < delta.retained_bytes_released() {
                return Err(SessionError::InvariantViolation(
                    "global replay byte accounting underflow",
                ));
            }
            if usage.segments < delta.segments_released() {
                return Err(SessionError::InvariantViolation(
                    "global replay segment accounting underflow",
                ));
            }
            if let Some(check) = finish_check {
                check?;
            }
            effects.prepare_ordinary()?;
            effects.mark_mutation();
            prepared.commit()
        };
        self.release_replay_usage(
            direction,
            acknowledged.retained_bytes_released(),
            acknowledged.segments_released(),
        )?;
        let finished = self
            .flow_storage
            .get(&flow_id)
            .map(|flow| flow.send.final_accepted() && flow.receive_final_accepted)
            .unwrap_or(false);
        let accepted_final = self
            .flow_storage
            .get(&flow_id)
            .map(|flow| flow.send.final_accepted())
            .ok_or(SessionError::UnknownFlow(flow_id))?;
        let receipt = ReplayAcknowledged {
            local_flow,
            generation: self.current_leg.generation(),
            direction,
            previous_accepted: acknowledged.previous_peer_acked(),
            next_accepted: acknowledged.peer_acked(),
            released_bytes: acknowledged.retained_bytes_released(),
            final_accepted: accepted_final,
        };
        effects.push(SessionEffect::ReplayAcknowledged { receipt })?;
        if finished {
            let flow = self
                .flow_storage
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            let control = Record::Ack {
                flow_id,
                direction: self.role.peer_receive_direction(),
                next_accepted: flow.receive.accepted(),
                final_accepted: true,
            };
            self.finish_flow(flow_id, FlowFinishReason::Graceful, Some(control), effects)?;
        }
        Ok(())
    }

    fn receive_close(
        &mut self,
        flow_id: SessionFlowId,
        direction: Direction,
        final_offset: ByteOffset,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.require_peer_receive_direction(direction)?;
        if let Some(tombstone) = self.flow_storage.get_terminal(&flow_id) {
            return self.replay_terminal_control(tombstone, effects);
        }
        self.precheck_flow_live(flow_id)?;
        let may_request_half_close = {
            let flow = self
                .flow_storage
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            flow.sink_is_open()
                && final_offset == flow.receive.accepted()
                && flow.pending_half_close.is_none()
                && !flow.receive_final_accepted
        };
        if may_request_half_close && self.next_half_close_id.is_none() {
            return Err(SessionError::HalfCloseCompletionIdExhausted);
        }
        let active = self.phase == SessionPhase::Active;
        let (accepted, half_close_ready, final_accepted, sink_is_open) = {
            let flow = self.flow_mut(flow_id)?;
            let prepared = flow
                .receive
                .prepare_receive_close(final_offset)
                .map_err(|source| SessionError::TcpOwnership { flow_id, source })?;
            if active || (may_request_half_close && prepared.outcome().half_close_ready()) {
                effects.prepare_ordinary()?;
            }
            effects.mark_mutation();
            let close = prepared.commit();
            (
                flow.receive.accepted(),
                close.half_close_ready(),
                flow.receive_final_accepted,
                flow.sink_is_open(),
            )
        };
        self.transmit_if_active(
            Record::Ack {
                flow_id,
                direction,
                next_accepted: accepted,
                final_accepted,
            },
            effects,
        )?;
        if sink_is_open
            && half_close_ready
            && !final_accepted
            && let Some(effect) = self.begin_half_close(flow_id, effects)?
        {
            effects.push(effect)?;
        }
        Ok(())
    }

    fn receive_reset(
        &mut self,
        flow_id: SessionFlowId,
        reason: ResetReason,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        if let Some(tombstone) = self.flow_storage.get_terminal(&flow_id) {
            return self.replay_terminal_control(tombstone, effects);
        }
        self.precheck_flow_live(flow_id)?;
        self.precheck_flow_finish(flow_id)?;
        effects.prepare_ordinary()?;
        effects.push(SessionEffect::PeerReset { flow_id, reason })?;
        self.finish_flow(flow_id, FlowFinishReason::PeerReset(reason), None, effects)?;
        Ok(())
    }

    fn sink_accepted(
        &mut self,
        offer: SinkOffer,
        bytes: usize,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.validate_sink_offer(offer)?;
        if bytes == 0 {
            return Err(SessionError::ZeroSinkAcceptance);
        }
        if bytes > offer.len {
            return Err(SessionError::SinkAcceptanceTooLarge {
                offered: offer.len,
                attempted: bytes,
            });
        }
        if self.next_sink_offer_id.is_none() {
            let contiguous = self
                .flow_storage
                .get(&offer.flow_id)
                .ok_or(SessionError::UnknownFlow(offer.flow_id))?
                .receive
                .contiguous_len(usize::MAX);
            if contiguous > bytes {
                return Err(SessionError::SinkOfferIdExhausted);
            }
        }

        let (accept_preview, will_request_half_close) = {
            let flow = self
                .flow_storage
                .get(&offer.flow_id)
                .ok_or(SessionError::UnknownFlow(offer.flow_id))?;
            let preview = flow.receive.preview_accept(bytes).map_err(|source| {
                SessionError::TcpOwnership {
                    flow_id: offer.flow_id,
                    source,
                }
            })?;
            (
                preview,
                flow.receive.final_offset() == Some(preview.accepted())
                    && flow.pending_half_close.is_none()
                    && !flow.receive_final_accepted,
            )
        };
        if accept_preview.accepted_bytes() != bytes {
            return Err(SessionError::InvariantViolation(
                "sink offer exceeds contiguous TCP receive ownership",
            ));
        }
        if will_request_half_close && self.next_half_close_id.is_none() {
            return Err(SessionError::HalfCloseCompletionIdExhausted);
        }
        if self.receive_usage.bytes < bytes
            || self.receive_usage.segments < accept_preview.ranges_released()
        {
            return Err(SessionError::InvariantViolation(
                "sink acceptance exceeds global receive ownership",
            ));
        }

        let active = self.phase == SessionPhase::Active;
        let next_usage = ReplayBudgetUsage {
            bytes: self.receive_usage.bytes.checked_sub(bytes).ok_or(
                SessionError::InvariantViolation("global receive byte accounting underflow"),
            )?,
            segments: self
                .receive_usage
                .segments
                .checked_sub(accept_preview.ranges_released())
                .ok_or(SessionError::InvariantViolation(
                    "global receive range accounting underflow",
                ))?,
        };
        let (accepted, half_close_ready, final_accepted, sink_offer) = {
            let flow = self
                .flow_storage
                .get_mut(&offer.flow_id)
                .ok_or(SessionError::UnknownFlow(offer.flow_id))?;
            let sink_is_open = flow.sink_is_open();
            let prepared = flow.receive.prepare_accept(bytes).map_err(|source| {
                SessionError::TcpOwnership {
                    flow_id: offer.flow_id,
                    source,
                }
            })?;
            debug_assert_eq!(
                prepared.outcome().accepted_bytes(),
                accept_preview.accepted_bytes()
            );
            debug_assert_eq!(
                prepared.outcome().ranges_released(),
                accept_preview.ranges_released()
            );
            let segments = if sink_is_open {
                prepared
                    .prepare_offer(self.config.max_sink_offer_bytes())
                    .map_err(|source| SessionError::TcpOwnership {
                        flow_id: offer.flow_id,
                        source,
                    })?
            } else {
                Vec::new()
            };
            let sink_offer = PreparedSinkOffer::new(
                self.session_id,
                self.role.peer_receive_direction(),
                offer.flow_id,
                self.next_sink_offer_id,
                segments,
            )?;
            if active || will_request_half_close || sink_offer.is_some() {
                effects.prepare_ordinary()?;
            }
            effects.mark_mutation();
            let accepted = prepared.commit();
            flow.pending_sink_offer = None;
            (
                accepted.accepted(),
                flow.receive.half_close_ready(),
                flow.receive_final_accepted,
                sink_offer,
            )
        };
        self.receive_usage = next_usage;

        self.transmit_if_active(
            Record::Ack {
                flow_id: offer.flow_id,
                direction: offer.direction,
                next_accepted: accepted,
                final_accepted,
            },
            effects,
        )?;
        if half_close_ready
            && !final_accepted
            && let Some(effect) = self.begin_half_close(offer.flow_id, effects)?
        {
            effects.push(effect)?;
        }
        self.publish_sink_offer(sink_offer, effects)?;
        Ok(())
    }

    fn sink_abandoned(
        &mut self,
        offer: SinkOffer,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.validate_sink_offer(offer)?;
        self.precheck_flow_finish(offer.flow_id)?;
        let control = Record::Reset {
            flow_id: offer.flow_id,
            reason: ResetReason::LocalAbandon,
        };
        self.transmit_if_active(control.clone(), effects)?;
        self.finish_flow(
            offer.flow_id,
            FlowFinishReason::LocalReset(ResetReason::LocalAbandon),
            Some(control),
            effects,
        )?;
        Ok(())
    }

    fn sink_half_closed(
        &mut self,
        completion: SinkHalfClose,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let flow_id = self.validate_half_close(completion)?;
        let will_finish = self
            .flow_storage
            .get(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?
            .send
            .final_accepted();
        if will_finish {
            self.precheck_flow_finish(flow_id)?;
        }
        if self.phase == SessionPhase::Active || will_finish {
            effects.prepare_ordinary()?;
        }
        let accepted = {
            let flow = self.flow_mut(flow_id)?;
            effects.mark_mutation();
            flow.pending_half_close = None;
            flow.receive_final_accepted = true;
            flow.receive.accepted()
        };
        let control = Record::Ack {
            flow_id,
            direction: completion.direction,
            next_accepted: accepted,
            final_accepted: true,
        };
        self.transmit_if_active(control.clone(), effects)?;
        if will_finish {
            self.finish_flow(flow_id, FlowFinishReason::Graceful, Some(control), effects)?;
        }
        Ok(())
    }

    fn sink_half_close_failed(
        &mut self,
        completion: SinkHalfClose,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let flow_id = self.validate_half_close(completion)?;
        self.precheck_flow_finish(flow_id)?;
        let reason = ResetReason::LocalAbandon;
        let control = Record::Reset { flow_id, reason };
        self.transmit_if_active(control.clone(), effects)?;
        self.finish_flow(
            flow_id,
            FlowFinishReason::LocalReset(reason),
            Some(control),
            effects,
        )?;
        Ok(())
    }

    fn activate_sink_after_open(
        &mut self,
        flow_id: SessionFlowId,
        sink_offer: Option<PreparedSinkOffer>,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.publish_sink_offer(sink_offer, effects)?;
        let should_half_close = {
            let flow = self
                .flow_storage
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            flow.receive.half_close_ready()
                && flow.pending_half_close.is_none()
                && !flow.receive_final_accepted
        };
        if should_half_close && let Some(effect) = self.begin_half_close(flow_id, effects)? {
            effects.push(effect)?;
        }
        Ok(())
    }

    fn publish_sink_offer(
        &mut self,
        prepared: Option<PreparedSinkOffer>,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let Some(PreparedSinkOffer { offer, segments }) = prepared else {
            return Ok(());
        };
        effects.mark_mutation();
        self.next_sink_offer_id = offer.id.checked_add(1);
        self.flow_mut(offer.flow_id)?.pending_sink_offer = Some(offer);
        {
            effects.push(SessionEffect::OfferToSink { offer, segments })?;
            Ok(())
        }
    }

    fn begin_half_close(
        &mut self,
        flow_id: SessionFlowId,
        effects: &mut SessionEffectBatch,
    ) -> Result<Option<SessionEffect>, SessionError> {
        let flow = self
            .flow_storage
            .get(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?;
        if flow.receive_final_accepted || flow.pending_half_close.is_some() {
            return Ok(None);
        }
        if !flow.receive.half_close_ready() {
            return Err(SessionError::InvariantViolation(
                "half-close requested before final bytes were accepted",
            ));
        }
        let id = self
            .next_half_close_id
            .ok_or(SessionError::HalfCloseCompletionIdExhausted)?;
        let completion = SinkHalfClose {
            session_id: self.session_id,
            flow_id,
            direction: self.role.peer_receive_direction(),
            final_offset: flow.receive.accepted(),
            completion_id: id,
        };
        effects.mark_mutation();
        self.next_half_close_id = id.checked_add(1);
        self.flow_mut(flow_id)?.pending_half_close = Some(completion);
        Ok(Some(SessionEffect::HalfCloseSink { completion }))
    }
}
