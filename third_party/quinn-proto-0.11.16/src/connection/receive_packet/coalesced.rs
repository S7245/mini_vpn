//! A synchronous datagram transaction at the original Connection entry. Holds
//! original ciphertext/plaintext and actual preparations, never a second
//! Connection or retry queue. Unsupported shapes resume the original handler
//! with their actual decoded values. Tests: admission/packet_pressure/coalesced.
use super::*;
use bytes::{BackingMeter, MeteredCopyError};
use control_inventory::records::CryptoEntry;
use recovery::PrepareError;

enum Incoming {
    Wire(PartialDecode),
    Decoded { value: DecryptedPacket, reset: bool },
    Ignored,
}
impl Connection {
    pub(in crate::connection) fn packet_batch_meter(
        &self,
        remote: SocketAddr,
    ) -> Option<BackingMeter> {
        if !self.state.is_established() || !self.side.is_client() || remote != self.path.remote {
            return None;
        }
        self.endpoint_config
            .receive_backing_meter
            .as_ref()
            .filter(|m| m.budget_limit().is_some())
            .cloned()
    }

    /// True means the complete datagram was refused without protocol effects.
    pub(in crate::connection) fn handle_packet_batch(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        ecn: Option<EcnCodepoint>,
        first: PartialDecode,
        remaining: BytesMut,
        meter: BackingMeter,
    ) -> bool {
        let mut packets = stock::Stock::default();
        if let Err(error) = packets.push(Incoming::Wire(first), &meter) {
            return self.settle_batch_preparation(now, remote, ecn, error.into());
        }
        let mut remaining = Some(remaining);
        // Descriptor stock is outer datagram admission, before header/body
        // authentication. Full drops the complete original datagram, like its
        // pre-Proto admission; it cannot publish any authenticated packet.
        while let Some(data) = remaining.take() {
            match PartialDecode::new(
                data,
                &FixedLengthConnectionIdParser::new(self.local_cid_state.cid_len()),
                &[self.version],
                self.endpoint_config.grease_quic_bit,
            ) {
                Ok((packet, rest)) => {
                    if let Err(error) = packets.push(Incoming::Wire(packet), &meter) {
                        return self.settle_batch_preparation(now, remote, ecn, error.into());
                    }
                    remaining = rest;
                }
                Err(error) => {
                    trace!("malformed coalesced header: {}", error);
                    break;
                }
            }
        }
        let mut rx = self.spaces.each_ref().map(|s| s.rx_packet);
        let mut supported = true;
        for index in 0..packets.slice().len() {
            let Some(Incoming::Wire(partial)) = packets.slice_mut()[index].take() else {
                meter.quarantine();
                return self.settle_batch_preparation(
                    now,
                    remote,
                    ecn,
                    MeteredCopyError::Quarantined.into(),
                );
            };
            let Some(decoded) = packet_crypto::unprotect_header(
                partial,
                &self.spaces,
                self.zero_rtt_crypto.as_ref(),
                self.peer_params.stateless_reset_token,
            ) else {
                packets.slice_mut()[index] = Some(Incoming::Ignored);
                continue;
            };
            let value = match decoded.packet {
                None => Err(None),
                Some(mut packet) => {
                    let previous = rx[packet.header.space() as usize];
                    packet_crypto::decrypt_packet_body_at(
                        &mut packet,
                        &self.spaces,
                        self.zero_rtt_crypto.as_ref(),
                        self.key_phase,
                        self.prev_crypto.as_ref(),
                        self.next_crypto.as_ref(),
                        previous,
                    )
                    .map(move |auth| (packet.freeze(), auth))
                }
            };
            let preparation = match &value {
                Ok((packet, Some(auth))) if !decoded.stateless_reset => {
                    let space = packet.header.space();
                    // Duplication within this datagram is also read-only during
                    // preparation. The original dedup still commits in wire order.
                    let duplicate = self.spaces[space].dedup.contains(auth.number)
                        || packets.slice()[..index]
                            .iter()
                            .flatten()
                            .any(|old| match old {
                                Incoming::Decoded {
                                    value: Ok((p, Some(a))),
                                    reset: false,
                                } => p.header.space() == space && a.number == auth.number,
                                _ => false,
                            });
                    if duplicate {
                        Ok(true)
                    } else {
                        rx[space as usize] = rx[space as usize].max(auth.number);
                        if space == SpaceId::Data {
                            self.prepared_packet_shape(remote, packet, auth)
                                .map(|shape| shape.is_some())
                                .map_err(PrepareError::from)
                        } else {
                            self.check_early_packet(packet, &meter)
                        }
                    }
                }
                _ => Ok(false),
            };
            packets.slice_mut()[index] = Some(Incoming::Decoded {
                value,
                reset: decoded.stateless_reset,
            });
            match preparation {
                Ok(true) => {}
                Ok(false) => {
                    supported = false;
                    break;
                }
                Err(error) => return self.settle_batch_preparation(now, remote, ecn, error),
            }
        }
        if !supported {
            // No effects yet. Release actual preparations before resuming the
            // original path, including decrypting subsequent untouched packets
            // against the state produced by earlier packets.
            for packet in packets.slice_mut().iter_mut().filter_map(Option::take) {
                match packet {
                    Incoming::Wire(partial) => {
                        self.handle_decode(now, remote, ecn, partial, false);
                    }
                    Incoming::Decoded { value, reset } => {
                        self.handle_decrypted_packet(now, remote, ecn, value, reset, false);
                    }
                    Incoming::Ignored => {}
                }
            }
            return false;
        }
        // Every packet shape and every early-space protocol condition is now
        // qualified. Prepare DATA first so its complete protocol validation
        // precedes any early recovery workspace or TLS-record capacity refusal.
        let mut prepared_data = None;
        for (index, packet) in packets.slice().iter().enumerate().filter_map(|(i, p)| p.as_ref().map(|p| (i, p))) {
            if let Incoming::Decoded {
                value: Ok((packet, Some(auth))),
                reset: false,
            } = packet
            {
                if packet.header.space() == SpaceId::Data
                    && !self.spaces[SpaceId::Data].dedup.contains(auth.number)
                {
                    let prefix = &packets.slice()[..index];
                    let early = prefix.iter().enumerate().filter_map(|(i, value)| {
                        let Some(Incoming::Decoded { value: Ok((p, Some(a))), reset: false }) = value else { return None; };
                        let space = p.header.space();
                        let duplicate = prefix[..i].iter().flatten().any(|old| matches!(old,
                            Incoming::Decoded { value: Ok((q, Some(b))), reset: false }
                                if q.header.space() == space && b.number == a.number));
                        (!duplicate && space != SpaceId::Data).then_some((space, a.number, &p.payload))
                    });
                    match self.prepare_packet(now, remote, packet, auth, early) {
                        Ok(PacketAdmission::Prepared(prepared)) => prepared_data = Some(prepared),
                        Ok(PacketAdmission::Refused) => {
                            return self.settle_batch_preparation(
                                now,
                                remote,
                                ecn,
                                MeteredCopyError::BudgetFull.into(),
                            )
                        }
                        Ok(PacketAdmission::Original) => {
                            return self.settle_batch_preparation(
                                now,
                                remote,
                                ecn,
                                MeteredCopyError::Quarantined.into(),
                            )
                        }
                        Err(error) => {
                            return self.settle_batch_preparation(now, remote, ecn, error.into())
                        }
                    }
                }
            }
        }
        let mut idle_crypto = None;
        for packet in packets.slice().iter().flatten() {
            if let Incoming::Decoded {
                value: Ok((packet, Some(auth))),
                reset: false,
            } = packet
            {
                let space_id = packet.header.space();
                if space_id != SpaceId::Data && !self.spaces[space_id].dedup.contains(auth.number) {
                    if let Err(error) =
                        recovery::Recovery::prepare(self, space_id, &packet.payload, &meter)
                    {
                        return self.settle_batch_preparation(now, remote, ecn, error);
                    }
                    if idle_crypto.is_none() {
                        if let Err(error) = meter.admit_precharged() {
                            return self.settle_batch_preparation(now, remote, ecn, error.into());
                        }
                        match self.spaces[self.highest_space].pending.crypto.prepare() {
                            Ok(flight) => idle_crypto = Some(flight),
                            Err(()) => {
                                let error = if meter.is_quarantined() {
                                    MeteredCopyError::Quarantined
                                } else {
                                    MeteredCopyError::BudgetFull
                                };
                                return self.settle_batch_preparation(
                                    now,
                                    remote,
                                    ecn,
                                    error.into(),
                                );
                            }
                        }
                    }
                }
            }
        }
        let commit = PacketCommit {
            meter: meter.clone(),
            complete: false,
        };
        for packet in packets.slice_mut().iter_mut().filter_map(Option::take) {
            let Incoming::Decoded {
                value: Ok((packet, Some(auth))),
                reset: false,
            } = packet
            else {
                continue;
            };
            let space = packet.header.space();
            if self.spaces[space].dedup.contains(auth.number) {
                self.handle_decrypted_packet(
                    now,
                    remote,
                    ecn,
                    Ok((packet, Some(auth))),
                    false,
                    false,
                );
                continue;
            }
            let result = if space == SpaceId::Data {
                match prepared_data.take() {
                    Some(prepared) => {
                        self.commit_prepared_packet(now, remote, ecn, &packet, auth, prepared)
                    }
                    None => Err(TransportError::INTERNAL_ERROR(
                        "coalesced DATA preparation missing",
                    )),
                }
            } else {
                match idle_crypto.as_mut() {
                    Some(flight) => self.commit_early_packet(now, ecn, packet, auth, flight),
                    None => Err(TransportError::INTERNAL_ERROR(
                        "coalesced TLS output custody missing",
                    )),
                }
            };
            if let Err(error) = result {
                meter.quarantine(); // before error/close settlement can dispose original owners
                self.handle_decrypted_packet(now, remote, ecn, Err(Some(error)), false, false);
                return false; // commit guard retains sticky quarantine
            }
            self.stats.udp_rx.ios += 1;
        }
        commit.finish();
        false
    }

    fn check_early_packet(
        &self,
        packet: &Packet<Bytes>,
        meter: &BackingMeter,
    ) -> Result<bool, PrepareError> {
        let space_id = packet.header.space();
        let space = &self.spaces[space_id];
        for frame in frame::Iter::new(packet.payload.clone())? {
            match frame.map_err(TransportError::from)? {
                Frame::Padding | Frame::Ping | Frame::Ack(_) => {}
                Frame::Crypto(crypto) => {
                    let end = crypto
                        .offset
                        .checked_add(crypto.data.len() as u64)
                        .ok_or_else(|| {
                            TransportError::PROTOCOL_VIOLATION("CRYPTO offset overflow")
                        })?;
                    if end > space.crypto_stream.bytes_read() {
                        return Err(TransportError::PROTOCOL_VIOLATION(
                            "new data at unexpected encryption level",
                        )
                        .into());
                    }
                    // Established early-space CRYPTO is only a duplicate. An
                    // unexpected retained assembler cannot be assumed inert.
                    if space.crypto_stream.progress().buffered_bytes != 0 {
                        return Ok(false);
                    }
                    space
                        .crypto_stream
                        .qualify_read(Some(meter))
                        .map_err(|_| MeteredCopyError::Quarantined)?;
                    space
                        .crypto_stream
                        .check_input_authority(&crypto.data, Some(meter))
                        .map_err(|_| MeteredCopyError::Quarantined)?;
                }
                _ => return Ok(false),
            }
        }
        space
            .pending_acks
            .ranges
            .prepare_cold_insert(meter)
            .map_err(|_| MeteredCopyError::Quarantined)?;
        recovery::Recovery::check(self, space_id, &packet.payload)?;
        // Early spaces cannot carry application stream or successor records.
        // This is a code invariant of their original transmit path; recovery
        // therefore needs no application event or send-range installation.
        let query = PacketCommit {
            meter: meter.clone(),
            complete: false,
        };
        let idle = self.crypto.handshake_output_is_idle();
        query.finish();
        if idle != Some(true) {
            return Ok(false);
        }

        Ok(true)
    }

    fn commit_early_packet(
        &mut self,
        now: Instant,
        ecn: Option<EcnCodepoint>,
        packet: Packet<Bytes>,
        auth: packet_crypto::DecryptPacketResult,
        flight: &mut CryptoEntry,
    ) -> Result<(), TransportError> {
        let space = packet.header.space();
        let number = auth.number;
        self.commit_packet_authentication(now, space, auth);
        self.spaces[space].dedup.insert(number);
        self.on_packet_authenticated(now, space, ecn, Some(number), false, false)?;
        self.process_early_payload(now, packet, Some(flight))
    }

    fn settle_batch_preparation(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        ecn: Option<EcnCodepoint>,
        error: PrepareError,
    ) -> bool {
        match error {
            PrepareError::Capacity(MeteredCopyError::BudgetFull) => {
                self.stats.udp_rx.admission_refused =
                    self.stats.udp_rx.admission_refused.saturating_add(1);
                true
            }
            PrepareError::Capacity(error) => {
                if matches!(error, MeteredCopyError::Quarantined) {
                    if let Some(meter) = self.endpoint_config.receive_backing_meter.as_ref() {
                        meter.quarantine();
                    }
                }
                self.handle_decrypted_packet(
                    now,
                    remote,
                    ecn,
                    Err(Some(TransportError::INTERNAL_ERROR(
                        streams::inventory_reason(error),
                    ))),
                    false,
                    false,
                );
                false
            }
            PrepareError::Protocol(error) => {
                self.handle_decrypted_packet(now, remote, ecn, Err(Some(error)), false, false);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
