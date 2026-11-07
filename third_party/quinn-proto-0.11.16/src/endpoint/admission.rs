//! Owns the Endpoint datagram decode and routing transaction.
//! It does not own socket I/O, Connection event delivery, or Root settlement.
//! `Endpoint::handle` preserves the original one-call route and response behavior;
//! true receive and QUIC regression tests live in `tests/quinn_metered_udp.rs`.

use super::*;

/// A routed datagram whose connection event retains the caller's admission.
/// Other Endpoint results do not consume a connection event slot.
#[doc(hidden)]
pub enum AdmittedDatagram<T> {
    Connection(ConnectionHandle, ConnectionEvent, T),
    Other(DatagramEvent),
}

impl Endpoint {
    /// Process an incoming UDP datagram
    pub fn handle(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        local_ip: Option<IpAddr>,
        ecn: Option<EcnCodepoint>,
        data: BytesMut,
        buf: &mut Vec<u8>,
    ) -> Option<DatagramEvent> {
        self.handle_with_admission(now, remote, local_ip, ecn, data, buf, |_| Some(()))
            .map(|admitted| match admitted {
                AdmittedDatagram::Connection(ch, event, ()) => {
                    DatagramEvent::ConnectionEvent(ch, event)
                }
                AdmittedDatagram::Other(event) => event,
            })
    }

    /// Decode and route a datagram, giving the caller one chance to obtain
    /// connection event capacity before the event enters connection custody.
    /// Rejection leaves the connection and Endpoint index unchanged. A granted
    /// token travels with the returned connection event and must be consumed
    /// exactly once by the caller's queue owner.
    #[doc(hidden)]
    pub fn handle_with_admission<'a, T>(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        local_ip: Option<IpAddr>,
        ecn: Option<EcnCodepoint>,
        data: BytesMut,
        buf: impl Into<ResponseBuffer<'a>>,
        admit_connection: impl FnOnce(ConnectionHandle) -> Option<T>,
    ) -> Option<AdmittedDatagram<T>> {
        let mut response = buf.into();
        if !response.validate(self, data.len()) {
            return None;
        }
        // Partially decode packet or short-circuit if unable
        let datagram_len = data.len();
        let event = match PartialDecode::new(
            data,
            &FixedLengthConnectionIdParser::new(self.local_cid_generator.cid_len()),
            &self.config.supported_versions,
            self.config.grease_quic_bit,
        ) {
            Ok((first_decode, remaining)) => DatagramConnectionEvent {
                now,
                remote,
                ecn,
                first_decode,
                remaining,
            },
            Err(PacketDecodeError::UnsupportedVersion {
                src_cid,
                dst_cid,
                version,
            }) => {
                if self.server_config.is_none() {
                    debug!("dropping packet with unsupported version");
                    return None;
                }
                trace!("sending version negotiation");
                let buf = response.legacy()?;
                // Negotiate versions
                Header::VersionNegotiate {
                    random: self.rng.random::<u8>() | 0x40,
                    src_cid: dst_cid,
                    dst_cid: src_cid,
                }
                .encode(buf);
                // Grease with a reserved version
                buf.write::<u32>(match version {
                    0x0a1a_2a3a => 0x0a1a_2a4a,
                    _ => 0x0a1a_2a3a,
                });
                for &version in &self.config.supported_versions {
                    buf.write(version);
                }
                return Some(AdmittedDatagram::Other(DatagramEvent::Response(Transmit {
                    destination: remote,
                    ecn: None,
                    size: buf.len(),
                    segment_size: None,
                    src_ip: local_ip,
                })));
            }
            Err(e) => {
                trace!("malformed header: {}", e);
                return None;
            }
        };

        let addresses = FourTuple { remote, local_ip };
        let dst_cid = event.first_decode.dst_cid();

        let result =
            if let Some(route_to) = self.registrations.route(&addresses, &event.first_decode) {
                // Handle packet on existing connection
                match route_to {
                    RouteDatagramTo::Incoming(incoming_idx) => {
                        let incoming_buffer = &mut self.incoming_buffers[incoming_idx];
                        let config = &self.server_config.as_ref().unwrap();

                        if incoming_buffer
                            .total_bytes
                            .checked_add(datagram_len as u64)
                            .is_some_and(|n| n <= config.incoming_buffer_size)
                            && self
                                .all_incoming_buffers_total_bytes
                                .checked_add(datagram_len as u64)
                                .is_some_and(|n| n <= config.incoming_buffer_size_total)
                        {
                            incoming_buffer.datagrams.push(event);
                            incoming_buffer.total_bytes += datagram_len as u64;
                            self.all_incoming_buffers_total_bytes += datagram_len as u64;
                        }

                        None
                    }
                    RouteDatagramTo::Connection(ch) => {
                        return admit_connection(ch).map(|token| {
                            AdmittedDatagram::Connection(
                                ch,
                                ConnectionEvent(ConnectionEventInner::Datagram(event)),
                                token,
                            )
                        });
                    }
                }
            } else if event.first_decode.initial_header().is_some() {
                // Potentially create a new connection

                self.handle_first_packet(datagram_len, event, addresses, &mut response)
            } else if event.first_decode.has_long_header() {
                debug!(
                    "ignoring non-initial packet for unknown connection {}",
                    dst_cid
                );
                None
            } else if !event.first_decode.is_initial()
                && self.local_cid_generator.validate(dst_cid).is_err()
            {
                debug!("dropping packet with invalid CID");
                None
            } else if dst_cid.is_empty() {
                trace!("dropping unrecognized short packet without ID");
                None
            } else {
                // If we got this far, we're receiving a seemingly valid packet for an unknown
                // connection. Send a stateless reset if possible.
                self.stateless_reset(now, datagram_len, addresses, *dst_cid, &mut response)
                    .map(DatagramEvent::Response)
            };
        result.map(AdmittedDatagram::Other)
    }
}
