//! Original Connection control-frame execution. This is the shared wire-order
//! commit path, not a second protocol state machine. Inventories remain in
//! their original stream/path/CID/TLS owners; packet preparation is separate.
//! Entry: process_control_frame. Existing QUIC and admission tests cover it.
use super::*;

impl Connection {
    pub(super) fn process_control_frame(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        number: u64,
        frame: Frame,
    ) -> Result<Option<Close>, TransportError> {
        match frame {
            Frame::Close(reason) => {
                return Ok(Some(reason));
            }
            Frame::PathChallenge(token) => {
                let prepared = self.path_responses.prepare_push(remote, self.endpoint_config.receive_backing_meter.as_ref())
                    .map_err(application_events::event_error)?;
                self.path_responses.install(prepared).map_err(application_events::event_error)?;
                self.path_responses.push(number, token, remote);
                if remote == self.path.remote {
                    // PATH_CHALLENGE on active path, possible off-path packet forwarding
                    // attack. Send a non-probing packet to recover the active path.
                    match self.peer_supports_ack_frequency() {
                        true => self.immediate_ack(),
                        false => self.ping(),
                    }
                }
            }
            Frame::PathResponse(token) => {
                if self.path.challenge == Some(token) && remote == self.path.remote {
                    trace!("new path validated");
                    self.timers.stop(Timer::PathValidation);
                    self.path.challenge = None;
                    self.path.validated = true;
                    if let Some((_, ref mut prev_path)) = self.prev_path {
                        prev_path.challenge = None;
                        prev_path.challenge_pending = false;
                    }
                } else {
                    debug!(token, "ignoring invalid PATH_RESPONSE");
                }
            }
            Frame::MaxData(bytes) => {
                self.streams.received_max_data(bytes);
            }
            Frame::MaxStreamData { id, offset } => {
                self.streams.received_max_stream_data(id, offset)?;
            }
            Frame::MaxStreams { dir, count } => {
                self.streams.received_max_streams(dir, count)?;
            }
            Frame::ResetStream(frame) => {
                if self.streams.received_reset(frame)?.should_transmit() {
                    self.spaces[SpaceId::Data].pending.max_data = true;
                }
            }
            Frame::DataBlocked { offset } => {
                debug!(offset, "peer claims to be blocked at connection level");
            }
            Frame::StreamDataBlocked { id, offset } => {
                self.on_stream_data_blocked(id, offset)?;
            }
            Frame::StreamsBlocked { dir, limit } => {
                if limit > MAX_STREAM_COUNT {
                    return Err(TransportError::FRAME_ENCODING_ERROR(
                        "unrepresentable stream limit",
                    ));
                }
                debug!(
                    "peer claims to be blocked opening more than {} {} streams",
                    limit, dir
                );
            }
            Frame::StopSending(frame::StopSending { id, error_code }) => {
                if id.initiator() != self.side.side() {
                    if id.dir() == Dir::Uni {
                        debug!("got STOP_SENDING on recv-only {}", id);
                        return Err(TransportError::STREAM_STATE_ERROR(
                            "STOP_SENDING on recv-only stream",
                        ));
                    }
                } else if self.streams.is_local_unopened(id) {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "STOP_SENDING on unopened stream",
                    ));
                }
                if let Err(error) = self.streams.received_stop_sending(id, error_code) {
                    self.fail_control_inventory(now, streams::inventory_reason(error));
                    return Err(TransportError::INTERNAL_ERROR(streams::inventory_reason(
                        error,
                    )));
                }
            }
            Frame::RetireConnectionId { sequence } => {
                if let Some(allow_more_cids) = self
                    .local_cid_state
                    .on_cid_retirement(sequence, self.peer_params.issue_cids_limit())?
                {
                    self.endpoint_events
                        .push_back(EndpointEventInner::RetireConnectionId(
                            now,
                            sequence,
                            allow_more_cids,
                        ));
                }
            }
            Frame::NewConnectionId(frame) => {
                trace!(
                    sequence = frame.sequence,
                    id = %frame.id,
                    retire_prior_to = frame.retire_prior_to,
                );
                if self.rem_cids.active().is_empty() {
                    return Err(TransportError::PROTOCOL_VIOLATION(
                        "NEW_CONNECTION_ID when CIDs aren't in use",
                    ));
                }
                if frame.retire_prior_to > frame.sequence {
                    return Err(TransportError::PROTOCOL_VIOLATION(
                        "NEW_CONNECTION_ID retiring unissued CIDs",
                    ));
                }

                let prepared = self.prepare_remote_cid(Some(frame))?;
                self.commit_remote_cid(prepared, now);
            }
            Frame::NewToken(NewToken { token }) => {
                let ConnectionSide::Client {
                    token_store,
                    server_name,
                    ..
                } = &self.side
                else {
                    return Err(TransportError::PROTOCOL_VIOLATION("client sent NEW_TOKEN"));
                };
                if token.is_empty() {
                    return Err(TransportError::FRAME_ENCODING_ERROR("empty token"));
                }
                trace!("got new token");
                if let Some(meter) = self
                    .endpoint_config
                    .receive_backing_meter
                    .as_ref()
                    .filter(|m| m.budget_limit().is_some())
                {
                    token_store
                        .insert_metered(server_name, token, meter)
                        .map_err(|_| {
                            TransportError::INTERNAL_ERROR("token cache admission refused")
                        })?;
                } else {
                    token_store.insert(server_name, token);
                }
            }
            Frame::AckFrequency(ack_frequency) => {
                // This frame can only be sent in the Data space
                let space = &mut self.spaces[SpaceId::Data];

                if !self
                    .ack_frequency
                    .ack_frequency_received(&ack_frequency, &mut space.pending_acks)?
                {
                    // The AckFrequency frame is stale (we have already received a more recent one)
                    return Ok(None);
                }

                // Our `max_ack_delay` has been updated, so we may need to adjust its associated
                // timeout
                if let Some(timeout) = space
                    .pending_acks
                    .max_ack_delay_timeout(self.ack_frequency.max_ack_delay)
                {
                    self.timers.set(Timer::MaxAckDelay, timeout);
                }
            }
            Frame::ImmediateAck => {
                // This frame can only be sent in the Data space
                self.spaces[SpaceId::Data]
                    .pending_acks
                    .set_immediate_ack_required();
            }
            Frame::HandshakeDone => {
                if self.side.is_server() {
                    return Err(TransportError::PROTOCOL_VIOLATION(
                        "client sent HANDSHAKE_DONE",
                    ));
                }
                if self.spaces[SpaceId::Handshake].crypto.is_some() {
                    self.discard_space(now, SpaceId::Handshake);
                }
            }
            _ => {
                return Err(TransportError::INTERNAL_ERROR(
                    "non-control frame at control commit",
                ))
            }
        }
        Ok(None)
    }
}

/// Only controls whose effects and complete resource demand are prepared here.
pub(super) fn prepared_control(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::MaxData(_)
            | Frame::MaxStreamData { .. }
            | Frame::MaxStreams { .. }
            | Frame::DataBlocked { .. }
            | Frame::StreamDataBlocked { .. }
            | Frame::StreamsBlocked { .. }
            | Frame::PathResponse(_)
            | Frame::AckFrequency(_)
            | Frame::HandshakeDone
            | Frame::RetireConnectionId { .. }
            | Frame::NewToken(_)
            | Frame::StopSending(_)
            | Frame::PathChallenge(_)
            | Frame::NewConnectionId(_)
    )
}
impl Connection {
    pub(super) fn check_packet_controls(&self, payload: &Bytes) -> Result<(), TransportError> {
        let mut ack_frequency = self.ack_frequency.clone();
        for frame in frame::Iter::new(payload.clone())? {
            match frame? {
                Frame::RetireConnectionId { sequence } => self.local_cid_state.check_retirement(sequence)?,
                Frame::NewToken(NewToken { token }) => {
                    if self.side.is_server() { return Err(TransportError::PROTOCOL_VIOLATION("client sent NEW_TOKEN")); }
                    if token.is_empty() { return Err(TransportError::FRAME_ENCODING_ERROR("empty token")); }
                }
                Frame::AckFrequency(value) => {
                    ack_frequency.apply_received_value(&value)?;
                }
                Frame::StreamsBlocked { limit, .. } if limit > MAX_STREAM_COUNT => {
                    return Err(TransportError::FRAME_ENCODING_ERROR(
                        "unrepresentable stream limit",
                    ));
                }
                Frame::StreamDataBlocked { id, .. }
                    if id.initiator() == self.side.side() && id.dir() == Dir::Uni =>
                {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "STREAM_DATA_BLOCKED on send-only stream",
                    ));
                }
                Frame::HandshakeDone if self.side.is_server() => {
                    return Err(TransportError::PROTOCOL_VIOLATION(
                        "client sent HANDSHAKE_DONE",
                    ));
                }
                frame => self.streams.check_packet_flow_control(&frame)?,
            }
        }
        Ok(())
    }
}
