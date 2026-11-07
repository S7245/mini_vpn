//! Original Connection packet authentication, admission and error settlement.
//! Owns the receive commit point and known-Full refusal. Frame storage remains
//! in the original stream/ACK inventories; mixed ACK/STREAM shares their
//! preparation and preserves the original ACK/loss state machine. Other shapes retain
//! the original failstop path. No new task, queue, timer or refund owner.
//! Tests use real encrypted Endpoint events in tests/admission.
use super::*;
mod recovery;
mod coalesced;
use super::storage::stock;

type DecryptedPacket = Result<(Packet<Bytes>, Option<packet_crypto::DecryptPacketResult>), Option<TransportError>>;

enum PacketAdmission {
    Original,
    Refused,
    Prepared(PreparedPacket),
}

// Actual unpublished inventories owned by this synchronous Connection turn.
// Dropping preparation cancels only temporary physical resources; protocol
// publication starts exclusively in commit_prepared_packet.
struct PreparedShape { pings: u64, ack_eliciting: bool }

struct PreparedPacket {
    meter: bytes::BackingMeter,
    pings: u64,
    ack_eliciting: bool,
    recovery: recovery::Recovery,
    cids: cid_retirement::packet::PacketCids,
    path_response: Option<paths::ResponseCapacity>,
    crypto: Option<assembler::crypto::CryptoInsertion>,
    tokens: Option<crate::token::PreparedTokenInsert>,
    controls: streams::state::packet_controls::PacketControls,
    application_events: application_events::Prepared,
    datagrams: datagrams::packet::Prepared,
    operations: streams::state::inventory::Prepared,
    receive: streams::state::packet_receive::PacketReceives,
}

impl Connection {
    pub(super) fn handle_decode(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        ecn: Option<EcnCodepoint>,
        partial_decode: PartialDecode,
        standalone: bool,
    ) -> bool {
        if let Some(decoded) = packet_crypto::unprotect_header(
            partial_decode,
            &self.spaces,
            self.zero_rtt_crypto.as_ref(),
            self.peer_params.stateless_reset_token,
        ) {
            return self.handle_packet(
                now,
                remote,
                ecn,
                decoded.packet,
                decoded.stateless_reset,
                standalone,
            );
        }
        false
    }

    fn handle_packet(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        ecn: Option<EcnCodepoint>,
        packet: Option<Packet>,
        stateless_reset: bool,
        standalone: bool,
    ) -> bool {
        if let Some(ref packet) = packet {
            trace!(
                "got {:?} packet ({} bytes) from {} using id {}",
                packet.header.space(),
                packet.payload.len() + packet.header_data.len(),
                remote,
                packet.header.dst_cid(),
            );
        }

        if self.is_handshaking() && remote != self.path.remote {
            debug!("discarding packet with unexpected remote during handshake");
            return false;
        }

        let decrypted = match packet {
            None => Err(None),
            Some(mut packet) => packet_crypto::decrypt_packet_body(
                &mut packet,
                &self.spaces,
                self.zero_rtt_crypto.as_ref(),
                self.key_phase,
                self.prev_crypto.as_ref(),
                self.next_crypto.as_ref(),
            )
            .map(move |authentication| (packet.freeze(), authentication)),
        };
        self.handle_decrypted_packet(now, remote, ecn, decrypted, stateless_reset, standalone)
    }

    fn handle_decrypted_packet(
        &mut self, now: Instant, remote: SocketAddr, ecn: Option<EcnCodepoint>,
        decrypted: DecryptedPacket, stateless_reset: bool, standalone: bool,
    ) -> bool {
        let was_closed = self.state.is_closed();
        let was_drained = self.state.is_drained();
        // Complete authenticated packet admission precedes every protocol
        // mutation, including key effects, dedup, ACKs and idle reset.
        let decrypted = match decrypted {
            Ok((packet, Some(authentication))) if standalone && !stateless_reset => {
                match self.prepare_packet(now, remote, &packet, &authentication, std::iter::empty()) {
                    Ok(PacketAdmission::Refused) => {
                        self.stats.udp_rx.admission_refused =
                            self.stats.udp_rx.admission_refused.saturating_add(1);
                        return true;
                    }
                    Ok(PacketAdmission::Prepared(prepared)) => {
                        match self.commit_prepared_packet(now, remote, ecn, &packet, authentication, prepared) {
                            Ok(()) => {
                                self.stats.udp_rx.ios += 1;
                                return false;
                            }
                            Err(error) => Err(Some(error)),
                        }
                    }
                    Ok(PacketAdmission::Original) => Ok((packet, Some(authentication))),
                    Err(error) => Err(Some(error)),
                }
            }
            other => other,
        };
        self.stats.udp_rx.ios += 1;
        let result = match decrypted {
            _ if stateless_reset => {
                debug!("got stateless reset");
                Err(ConnectionError::Reset)
            }
            Err(Some(e)) => {
                warn!("illegal packet: {}", e);
                Err(e.into())
            }
            Err(None) => {
                debug!("failed to authenticate packet");
                self.authentication_failures += 1;
                let integrity_limit = self.spaces[self.highest_space]
                    .crypto
                    .as_ref()
                    .unwrap()
                    .packet
                    .local
                    .integrity_limit();
                if self.authentication_failures > integrity_limit {
                    Err(TransportError::AEAD_LIMIT_REACHED("integrity limit violated").into())
                } else {
                    return false;
                }
            }
            Ok((packet, authentication)) => {
                let number = authentication.map(|result| {
                    self.commit_packet_authentication(now, packet.header.space(), result)
                });
                let span = match number {
                    Some(pn) => trace_span!("recv", space = ?packet.header.space(), pn),
                    None => trace_span!("recv", space = ?packet.header.space()),
                };
                let _guard = span.enter();

                let is_duplicate = |n| self.spaces[packet.header.space()].dedup.insert(n);
                if number.is_some_and(is_duplicate) {
                    debug!("discarding possible duplicate packet");
                    return false;
                } else if self.state.is_handshake() && packet.header.is_short() {
                    // TODO: SHOULD buffer these to improve reordering tolerance.
                    trace!("dropping short packet during handshake");
                    return false;
                } else {
                    if let Header::Initial(InitialHeader { ref token, .. }) = packet.header {
                        if let State::Handshake(ref hs) = self.state {
                            if self.side.is_server() && token != &hs.expected_token {
                                // Clients must send the same retry token in every Initial. Initial
                                // packets can be spoofed, so we discard rather than killing the
                                // connection.
                                warn!("discarding Initial with invalid retry token");
                                return false;
                            }
                        }
                    }

                    let authenticated = if !self.state.is_closed() {
                        let spin = match packet.header {
                            Header::Short { spin, .. } => spin,
                            _ => false,
                        };
                        self.on_packet_authenticated(
                            now,
                            packet.header.space(),
                            ecn,
                            number,
                            spin,
                            packet.header.is_1rtt(),
                        )
                    } else {
                        Ok(())
                    };
                    authenticated
                        .map_err(ConnectionError::from)
                        .and_then(|()| self.process_decrypted_packet(now, remote, number, packet))
                }
            }
        };

        // State transitions for error cases
        if let Err(conn_err) = result {
            self.record_first_error(conn_err.clone());
            self.state = match conn_err {
                ConnectionError::ApplicationClosed(reason) => State::closed(reason),
                ConnectionError::ConnectionClosed(reason) => State::closed(reason),
                ConnectionError::Reset
                | ConnectionError::TransportError(TransportError {
                    code: TransportErrorCode::AEAD_LIMIT_REACHED,
                    ..
                }) => State::Drained,
                ConnectionError::TimedOut => {
                    unreachable!("timeouts aren't generated by packet processing");
                }
                ConnectionError::TransportError(err) => {
                    debug!("closing connection due to transport error: {}", err);
                    State::closed(err)
                }
                ConnectionError::VersionMismatch => State::Draining,
                ConnectionError::LocallyClosed => {
                    unreachable!("LocallyClosed isn't generated by packet processing");
                }
                ConnectionError::CidsExhausted => {
                    unreachable!("CidsExhausted isn't generated by packet processing");
                }
            };
        }

        self.finish_received_state(now, remote, was_closed, was_drained);
        false
    }

    fn finish_received_state(&mut self, now: Instant, remote: SocketAddr, was_closed: bool, was_drained: bool) {
        if !was_closed && self.state.is_closed() {
            self.close_common();
            if !self.state.is_drained() {
                self.set_close_timer(now);
            }
        }
        if !was_drained && self.state.is_drained() {
            self.endpoint_events.push_back(EndpointEventInner::Drained);
            // Close timer may have been started previously, e.g. if we sent a close and got a
            // stateless reset in response
            self.timers.stop(Timer::Close);
        }

        // Transmit CONNECTION_CLOSE if necessary
        if let State::Closed(_) = self.state {
            self.close = remote == self.path.remote;
        }
    }

    /// Prepare all resources for the supported packet transaction. Unsupported
    /// shapes keep the original authenticated processing path.
    fn prepared_packet_shape(&self, remote: SocketAddr, packet: &Packet<Bytes>,
        authentication: &packet_crypto::DecryptPacketResult) -> Result<Option<PreparedShape>, TransportError> {
        if !self.state.is_established()
            || !packet.header.is_1rtt()
            || remote != self.path.remote
            || !self.side.is_client()
        {
            return Ok(None);
        }
        let number = authentication.number;
        let space = &self.spaces[SpaceId::Data];
        if space.dedup.contains(number) {
            return Ok(None);
        }
        let mut pings = 0u64;
        let mut supported = true;
        let mut ack_eliciting = false;
        for frame in frame::Iter::new(packet.payload.clone())? {
            let frame = frame?;
            ack_eliciting |= frame.is_ack_eliciting();
            match frame {
                // An invalid stream ID is a protocol error, not an unsupported
                // shape. Validate against the entry window before any earlier
                // retirement can create a grant that this peer has not received.
                Frame::Stream(_) | Frame::ResetStream(_) => {},
                Frame::Datagram(_) | Frame::Crypto(_) | Frame::Close(_) => {},
                Frame::Ping => pings += 1,
                Frame::ImmediateAck => {}
                Frame::Ack(_) => {},
                Frame::Padding => {}
                frame if control_frames::prepared_control(&frame) => {},
                _ => supported = false,
            }
        }
        if !supported {
            return Ok(None);
        }
        Ok(Some(PreparedShape { pings, ack_eliciting }))
    }

    fn prepare_packet<'a>(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        packet: &Packet<Bytes>,
        authentication: &packet_crypto::DecryptPacketResult,
        early: impl Iterator<Item = (SpaceId, u64, &'a Bytes)> + Clone,
    ) -> Result<PacketAdmission, TransportError> {
        let Some(meter) = self
            .endpoint_config
            .receive_backing_meter
            .as_ref()
            .filter(|meter| meter.budget_limit().is_some())
            .cloned()
        else {
            return Ok(PacketAdmission::Original);
        };
        let Some(PreparedShape { pings, ack_eliciting }) = self.prepared_packet_shape(remote, packet, authentication)? else {
            return Ok(PacketAdmission::Original);
        };
        self.check_packet_controls(&packet.payload)?;
        self.check_packet_cids(now, &packet.payload, early.clone())?;
        let space = &self.spaces[SpaceId::Data];
        assembler::crypto::CryptoInsertion::check(&space.crypto_stream, ||
            frame::Iter::new(packet.payload.clone()).into_iter().flatten().filter_map(|f| match f {
                Ok(Frame::Crypto(f)) => Some((f.offset, f.data.len())), _ => None,
            }), self.config.crypto_buffer_size).map_err(|e| match e {
                assembler::crypto::PrepareError::BufferLimit => TransportError::CRYPTO_BUFFER_EXCEEDED(""),
                _ => TransportError::INTERNAL_ERROR("CRYPTO range validation failed"),
            })?;
        space
            .pending_acks
            .ranges
            .prepare_cold_insert(&meter)
            .map_err(|_| TransportError::INTERNAL_ERROR("ACK range custody invariant failed"))?;
        let datagrams = self.datagrams.check_packet(&packet.payload, &self.config.datagram_receive_buffer_size)?;
        let receive_events = self.streams.check_packet_streams(&packet.payload, &meter)?;
        let recovery = match recovery::Recovery::prepare(self, SpaceId::Data, &packet.payload, &meter) {
            Ok(prepared) => prepared,
            Err(recovery::PrepareError::Capacity(bytes::MeteredCopyError::BudgetFull)) => {
                return Ok(PacketAdmission::Refused)
            }
            Err(recovery::PrepareError::Capacity(error)) => {
                return Err(TransportError::INTERNAL_ERROR(streams::inventory_reason(
                    error,
                )))
            }
            Err(recovery::PrepareError::Protocol(error)) => return Err(error),
        };
        let cids = match self.prepare_packet_cids(now, &packet.payload, &meter, early) {
            Ok(prepared) => prepared,
            Err(cid_retirement::packet::PrepareError::Capacity(bytes::MeteredCopyError::BudgetFull)) => return Ok(PacketAdmission::Refused),
            Err(cid_retirement::packet::PrepareError::Capacity(error)) => return Err(application_events::event_error(error)),
            Err(cid_retirement::packet::PrepareError::Protocol(error)) => return Err(error),
        };
        let path_response = if frame::Iter::new(packet.payload.clone())?.any(|f| matches!(f, Ok(Frame::PathChallenge(_)))) {
            match self.path_responses.prepare_push(remote, Some(&meter)) {
                Ok(prepared) => Some(prepared),
                Err(bytes::MeteredCopyError::BudgetFull) => return Ok(PacketAdmission::Refused),
                Err(error) => return Err(application_events::event_error(error)),
            }
        } else { None };
        let controls = match self.streams.prepare_packet_controls(&packet.payload, &meter) {
            Ok(controls) => controls,
            Err(bytes::MeteredCopyError::BudgetFull) => return Ok(PacketAdmission::Refused),
            Err(error) => return Err(TransportError::INTERNAL_ERROR(streams::inventory_reason(error))),
        };
        let has_crypto = frame::Iter::new(packet.payload.clone())?.any(|f| matches!(f, Ok(Frame::Crypto(_))));
        let crypto = if has_crypto {
            let inputs = frame::Iter::new(packet.payload.clone())?.filter_map(|f| match f {
                Ok(Frame::Crypto(f)) => Some((f.offset, f.data, packet.payload.len())), _ => None,
            });
            match assembler::crypto::CryptoInsertion::prepare(&self.spaces[SpaceId::Data].crypto_stream, inputs, &meter, self.config.crypto_buffer_size) {
                Ok(plan) => Some(plan),
                Err(assembler::crypto::PrepareError::Insert(assembler::InsertError::Capacity(bytes::MeteredCopyError::BudgetFull))) =>
                    return Ok(PacketAdmission::Refused),
                Err(assembler::crypto::PrepareError::BufferLimit) => return Err(TransportError::CRYPTO_BUFFER_EXCEEDED("")),
                Err(_) => return Err(TransportError::INTERNAL_ERROR("CRYPTO packet inventory preparation failed")),
            }
        } else { None };
        let tokens = if frame::Iter::new(packet.payload.clone())?.any(|f| matches!(f, Ok(Frame::NewToken(_)))) {
            let ConnectionSide::Client { token_store, server_name, .. } = &self.side else {
                return Err(TransportError::PROTOCOL_VIOLATION("client sent NEW_TOKEN"));
            };
            match token_store.clone().prepare_insert_metered(server_name, &meter) {
                Ok(plan) => Some(plan),
                Err(crate::token::TokenStoreError::Capacity(bytes::MeteredCopyError::BudgetFull)) => return Ok(PacketAdmission::Refused),
                // Preserve the original provider path where no preparation contract exists.
                Err(crate::token::TokenStoreError::Unsupported) => return Ok(PacketAdmission::Original),
                Err(_) => return Err(TransportError::INTERNAL_ERROR("token cache preparation failed")),
            }
        } else { None };
        let events = recovery.events.checked_add(controls.events)
            .and_then(|events| events.checked_add(receive_events))
            .ok_or_else(|| TransportError::INTERNAL_ERROR("packet event inventory overflow"))?;
        let application_events = match self.prepare_application_events(datagrams.events.checked_add(crypto.as_ref().map_or(0, |c| c.reads))
            .ok_or_else(|| TransportError::INTERNAL_ERROR("CRYPTO event inventory overflow"))?) {
            Ok(plan) => plan,
            Err(bytes::MeteredCopyError::BudgetFull) => return Ok(PacketAdmission::Refused),
            Err(error) => return Err(application_events::event_error(error)),
        };
        let datagrams = match self.datagrams.prepare_packet(datagrams) {
            Ok(plan) => plan,
            Err(bytes::MeteredCopyError::BudgetFull) => return Ok(PacketAdmission::Refused),
            Err(error) => return Err(application_events::event_error(error)),
        };
        let operations = match self.streams.prepare_operations(events, recovery.pending, controls.blocked) {
            Ok(prepared) => prepared,
            Err(bytes::MeteredCopyError::BudgetFull) => return Ok(PacketAdmission::Refused),
            Err(error) => {
                return Err(TransportError::INTERNAL_ERROR(streams::inventory_reason(
                    error,
                )))
            }
        };
        let Some(receive) = self.streams.prepare_packet_streams_with_retirements(&packet.payload, &meter, recovery.retirements)? else {
            return Ok(PacketAdmission::Refused);
        };
        Ok(PacketAdmission::Prepared(PreparedPacket {
            meter, pings, ack_eliciting, recovery, controls, application_events,
            datagrams, operations, receive, crypto, tokens, cids, path_response,
        }))
    }

    fn commit_prepared_packet(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        ecn: Option<EcnCodepoint>,
        packet: &Packet<Bytes>,
        authentication: packet_crypto::DecryptPacketResult,
        prepared: PreparedPacket,
    ) -> Result<(), TransportError> {
        let PreparedPacket { meter, pings, ack_eliciting, recovery, controls,
            application_events, datagrams, operations, mut receive, mut crypto, mut tokens, mut cids, path_response } = prepared;
        let number = authentication.number;
        // Both send recovery and receive data now own their actual resources.
        // No protocol processing or fallible budget admission preceded this.
        let commit = PacketCommit {
            meter: meter.clone(),
            complete: false,
        };
        let key_update = if authentication.incoming_key_update {
            Some(self.prepare_key_update()?)
        } else { None };
        if let Some(prepared) = path_response { self.path_responses.install(prepared).map_err(application_events::event_error)?; }
        receive.install(&mut self.streams)?;
        controls.install(&mut self.streams)
            .map_err(|error| TransportError::INTERNAL_ERROR(streams::inventory_reason(error)))?;
        self.events.commit(application_events).map_err(application_events::event_error)?;
        self.datagrams.install_packet(datagrams)?;
        recovery
            .install(&mut self.streams)
            .map_err(|error| TransportError::INTERNAL_ERROR(streams::inventory_reason(error)))?;
        self.streams
            .commit_operations(operations)
            .map_err(|error| TransportError::INTERNAL_ERROR(streams::inventory_reason(error)))?;
        // All fallible inventory admission and provider work is complete.
        // Keep the original outgoing confirmation then incoming rotation order.
        self.commit_packet_authentication(now, SpaceId::Data, packet_crypto::DecryptPacketResult {
            number, outgoing_key_update_acked: authentication.outgoing_key_update_acked,
            incoming_key_update: false,
        });
        if let Some(prepared) = key_update {
            self.install_key_update(prepared, Some((number, now)), true)?;
            self.set_key_discard_timer(now, SpaceId::Data);
        }
        // No budget-dependent operation remains after the commit point.
        self.spaces[SpaceId::Data].dedup.insert(number);
        let spin = match packet.header {
            Header::Short { spin, .. } => spin,
            _ => false,
        };
        self.on_packet_authenticated(now, SpaceId::Data, ecn, Some(number), spin, true)?;
        let mut close = None;
        for frame in frame::Iter::new(packet.payload.clone())? {
            match frame? {
                Frame::Close(reason) => { self.stats.frame_rx.record(&Frame::Close(reason.clone())); close = Some(reason); }
                Frame::Ack(ack) => {
                    self.stats.frame_rx.acks += 1;
                    self.on_ack_received(now, SpaceId::Data, ack)?;
                }
                Frame::Crypto(_) => {
                    let plan = crypto.as_mut().ok_or_else(|| TransportError::INTERNAL_ERROR("missing CRYPTO preparation"))?;
                    plan.insert_next(&mut self.spaces[SpaceId::Data].crypto_stream)
                        .map_err(|_| TransportError::INTERNAL_ERROR("CRYPTO prepared insertion lost"))?;
                    self.stats.frame_rx.crypto += 1;
                    self.drain_crypto(SpaceId::Data)?;
                    plan.check_drained(&self.spaces[SpaceId::Data].crypto_stream)
                        .map_err(|_| TransportError::INTERNAL_ERROR("CRYPTO prepared drain changed"))?;
                }
                Frame::NewConnectionId(_) => {
                    self.stats.frame_rx.new_connection_id += 1;
                    cids.commit_next(self, now)?;
                }
                Frame::NewToken(NewToken { token }) => {
                    self.stats.frame_rx.new_token += 1;
                    tokens.as_mut().ok_or_else(|| TransportError::INTERNAL_ERROR("missing token preparation"))?
                        .insert(token).map_err(|_| TransportError::INTERNAL_ERROR("prepared token cache commit failed"))?;
                }
                Frame::Stream(frame) => {
                    if receive.commit_frame(&mut self.streams, frame)?.should_transmit() { self.spaces[SpaceId::Data].pending.max_data = true; }
                    self.stats.frame_rx.stream += 1;
                }
                Frame::ResetStream(frame) => {
                    if receive.commit_reset(&mut self.streams, frame)?.should_transmit() { self.spaces[SpaceId::Data].pending.max_data = true; }
                    self.stats.frame_rx.reset_stream += 1;
                }
                Frame::Datagram(datagram) => {
                    self.stats.frame_rx.datagram += 1;
                    if self.datagrams.receive_prepared(datagram, &self.config.datagram_receive_buffer_size)? {
                        let held = self.application_event_holds();
                        self.events.push_reserved(QueuedEvent::DatagramReceived, held)
                            .map_err(|_| application_events::event_error(bytes::MeteredCopyError::Quarantined))?;
                    }
                }
                Frame::ImmediateAck => {
                    self.stats.frame_rx.immediate_ack += 1;
                    self.spaces[SpaceId::Data].pending_acks.set_immediate_ack_required();
                }
                Frame::Padding | Frame::Ping => {}
                frame if control_frames::prepared_control(&frame) => {
                    self.stats.frame_rx.record(&frame);
                    if self.process_control_frame(now, remote, number, frame)?.is_some() {
                        return Err(TransportError::INTERNAL_ERROR("unexpected close in prepared controls"));
                    }
                }
                _ => {
                    return Err(TransportError::INTERNAL_ERROR(
                        "prepared packet shape changed",
                    ))
                }
            }
        }
        self.stats.frame_rx.ping += pings;
        let space = &mut self.spaces[SpaceId::Data];
        if space
            .pending_acks
            .packet_received(now, number, ack_eliciting, &space.dedup)
        {
            if let Some(timeout) = space
                .pending_acks
                .max_ack_delay_timeout(self.ack_frequency.max_ack_delay)
            {
                self.timers.set(Timer::MaxAckDelay, timeout);
            }
        }
        self.streams
            .queue_max_stream_id(&mut self.spaces[SpaceId::Data].pending);
        if let Some(reason) = close {
            self.record_first_error(reason.into());
            self.state = State::Draining;
            self.close = true;
            self.finish_received_state(now, remote, false, false);
        }
        commit.finish();
        Ok(())
    }
}

// Unexpected interruption after the first physical install cannot be called a
// cancelled preparation. Keep original owners under sticky quarantine.
struct PacketCommit {
    meter: bytes::BackingMeter,
    complete: bool,
}
impl PacketCommit {
    fn finish(mut self) {
        self.complete = true;
    }
}
impl Drop for PacketCommit {
    fn drop(&mut self) {
        if !self.complete {
            self.meter.quarantine();
        }
    }
}

#[cfg(test)]
mod tests;
