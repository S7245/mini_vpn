use super::*;

impl Connection {
    pub(crate) fn packet_key_discard_for_probe(&self) -> Option<Instant> {
        self.timers.get(Timer::KeyDiscard)
    }
    pub(crate) fn packet_local_key_update_pending_for_probe(&self) -> bool {
        self.prev_crypto.as_ref().is_some_and(|keys| keys.end_packet.is_none())
    }

    pub(crate) fn recovery_requirements_for_packet_probe(&self, payload: &Bytes) -> (usize, usize) {
        let meter = self.endpoint_config.receive_backing_meter.as_ref().unwrap();
        let prepared = match recovery::Recovery::prepare(self, SpaceId::Data, payload, meter) {
            Ok(prepared) => prepared,
            Err(_) => panic!("actual recovery preparation failed"),
        };
        (prepared.events, prepared.pending)
    }

    pub(crate) fn key_phase_for_packet_probe(&self) -> bool {
        self.key_phase
    }

    pub(crate) fn idle_deadline_for_packet_probe(&self) -> Option<Instant> {
        self.timers.get(Timer::Idle)
    }

    pub(crate) fn packet_admission_snapshot_for_probe(&self) -> String {
        let space = &self.spaces[SpaceId::Data];
        format!(
            "{:?}",
            (
                (
                    self.total_authed_packets,
                    self.permit_idle_reset,
                    self.receiving_ecn,
                    self.spin,
                    self.key_phase,
                    self.key_phase_size,
                    self.authentication_failures
                ),
                (
                    space.rx_packet,
                    space.dedup.snapshot_for_packet_probe(),
                    space.pending_acks.snapshot_for_packet_probe(),
                    space.ecn_counters
                ),
                Timer::VALUES.map(|timer| self.timers.get(timer)),
                (
                    self.path.remote,
                    self.path.total_recvd,
                    self.path.validated,
                    self.path.challenge
                ),
                (
                    self.prev_crypto
                        .as_ref()
                        .map(|keys| (keys.end_packet, keys.update_unacked)),
                    self.next_crypto
                        .as_ref()
                        .map(|keys| &*keys.remote as *const _ as *const () as usize),
                    space
                        .crypto
                        .as_ref()
                        .map(|keys| &*keys.packet.remote as *const _ as *const () as usize)
                ),
                (
                    self.stats.udp_rx.ios,
                    self.stats.udp_rx.datagrams,
                    self.stats.udp_rx.bytes,
                    format!("{:?}", self.stats.frame_rx),
                    self.error.clone(),
                    self.local_cid_state.snapshot_for_packet_probe(),
                    (self.rem_cids.clone(), self.peer_params.stateless_reset_token, self.path_responses.snapshot_for_packet_probe()),
                    (self.events.snapshot(), self.packet_receive_instances_snapshot_for_probe(), self.packet_send_controls_snapshot_for_probe(), self.datagrams.recv_buffered,
                        self.datagrams.incoming.iter().map(|d| (d.data.len(), d.data.allocation_identity())).collect::<Vec<_>>()),
                    (
                        space.largest_acked_packet,
                        space.largest_acked_packet_sent,
                        space.sent_packets.len(),
                        self.path.in_flight.bytes,
                        space.loss_time,
                        self.pto_count
                    )
                ),
            )
        )
    }
}

impl Connection {
    pub(crate) fn unseen_handshake_packet_for_probe(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        let mut rest = Some(BytesMut::from(bytes));
        while let Some(bytes) = rest.take() {
            let (partial, next) = PartialDecode::new(bytes,
                &FixedLengthConnectionIdParser::new(self.local_cid_state.cid_len()),
                &[self.version], self.endpoint_config.grease_quic_bit).unwrap();
            rest = next;
            if partial.space() != Some(SpaceId::Handshake) { continue; }
            let raw = partial.data().to_vec();
            let decoded = packet_crypto::unprotect_header(partial, &self.spaces,
                self.zero_rtt_crypto.as_ref(), self.peer_params.stateless_reset_token)?;
            let mut packet = decoded.packet?;
            let auth = packet_crypto::decrypt_packet_body(&mut packet, &self.spaces,
                self.zero_rtt_crypto.as_ref(), self.key_phase, self.prev_crypto.as_ref(),
                self.next_crypto.as_ref()).unwrap().unwrap();
            assert!(!self.spaces[SpaceId::Handshake].dedup.contains(auth.number),
                "real PTO must use a new Handshake PN");
            assert!(frame::Iter::new(packet.payload.freeze()).unwrap()
                .any(|frame| matches!(frame, Ok(Frame::Crypto(_)))));
            return Some(raw);
        }
        None
    }
    pub(crate) fn packet_spaces_snapshot_for_probe(&self) -> String {
        format!("{:?}", self.spaces.iter().map(|s| (
            s.rx_packet, s.dedup.snapshot_for_packet_probe(),
            s.pending_acks.snapshot_for_packet_probe(), s.ecn_counters,
            s.crypto.as_ref().map(|c| &*c.packet.remote as *const _ as *const () as usize),
            s.crypto_stream.bytes_read(), s.crypto_offset, s.pending.crypto.len(),
            s.sent_packets.len(), s.largest_acked_packet, s.loss_time,
        )).collect::<Vec<_>>())
    }
}

impl Connection {
    pub(crate) fn packet_spaces_wire_for_probe(&self, bytes: &[u8]) -> Vec<(Option<SpaceId>, Vec<u8>)> {
        let mut result = Vec::new();
        let mut rest = Some(BytesMut::from(bytes));
        while let Some(bytes) = rest.take() {
            let (partial, next) = PartialDecode::new(bytes,
                &FixedLengthConnectionIdParser::new(self.local_cid_state.cid_len()),
                &[self.version], self.endpoint_config.grease_quic_bit).unwrap();
            result.push((partial.space(), partial.data().to_vec()));
            rest = next;
        }
        result
    }
}

impl Connection {
    pub(crate) fn packet_frame_types_for_probe(&self, bytes: &[u8]) -> Vec<frame::FrameType> {
        self.packet_frames_for_probe(bytes).iter().map(Frame::ty).collect()
    }
    pub(crate) fn packet_frames_for_probe(&self, bytes: &[u8]) -> Vec<Frame> {
        let (partial, rest) = PartialDecode::new(BytesMut::from(bytes),
            &FixedLengthConnectionIdParser::new(self.local_cid_state.cid_len()),
            &[self.version], self.endpoint_config.grease_quic_bit).unwrap();
        assert!(rest.is_none());
        let decoded = packet_crypto::unprotect_header(partial, &self.spaces,
            self.zero_rtt_crypto.as_ref(), self.peer_params.stateless_reset_token).unwrap();
        let mut packet = decoded.packet.unwrap();
        packet_crypto::decrypt_packet_body(&mut packet, &self.spaces,
            self.zero_rtt_crypto.as_ref(), self.key_phase, self.prev_crypto.as_ref(),
            self.next_crypto.as_ref()).unwrap().unwrap();
        frame::Iter::new(packet.payload.freeze()).unwrap().map(Result::unwrap).collect()
    }
}

impl Connection {
    /// Deliberately invalid sender for an encrypted protocol-error tracer. This
    /// is not a received MAX_STREAMS frame and is never used as positive evidence.
    pub(crate) fn overgrant_sender_for_packet_probe(&mut self, dir: Dir, count: u64) {
        self.streams.received_max_streams(dir, count).unwrap();
    }
}

impl Connection {
    /// Request a legal early-space PING through the original packet writer.
    /// No ciphertext, packet number, history, keys, or credit is synthesized.
    pub(crate) fn request_handshake_ping_for_probe(&mut self) {
        assert!(self.spaces[SpaceId::Handshake].crypto.is_some());
        assert!(self.spaces[SpaceId::Handshake].pending.crypto.is_empty());
        self.spaces[SpaceId::Handshake].ping_pending = true;
    }
}
