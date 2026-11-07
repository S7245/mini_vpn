//! Original Connection's complete TLS-output barrier. Metadata is held before
//! even a zero-byte/key-only call; exact backing and offset are admitted before
//! provider consumption. No second error/settlement authority. Pending, Retry
//! and packet recovery retain the same Pool records. Tests: admission/crypto.
use super::*;
use crate::crypto::HandshakeOutputError;
use control_inventory::records::CryptoEntry;

impl Connection {
    pub(super) fn write_crypto(&mut self) -> Result<(), TransportError> {
        loop {
            let space = self.highest_space;
            let offset = self.spaces[space].crypto_offset;
            let pending = &self.spaces[space].pending.crypto;
            let bounded = pending.budgeted();
            let mut flight = pending.prepare().map_err(|_| crypto_capacity())?;
            let save_hello = matches!(self.state, State::Handshake(_))
                && self.side.is_client()
                && space == SpaceId::Initial
                && offset == 0;
            let mut replay = if save_hello {
                Some(pending.prepare().map_err(|_| crypto_capacity())?)
            } else {
                None
            };
            let changed = if bounded {
                let meter = self
                    .endpoint_config
                    .receive_backing_meter
                    .as_ref()
                    .ok_or_else(crypto_capacity)?;
                flight.begin_mutation();
                match self.crypto.write_handshake_bounded(&mut []) {
                    Ok((0, changed)) => changed,
                    Ok(_) => {
                        meter.quarantine();
                        return Err(crypto_capacity());
                    }
                    Err(HandshakeOutputError::Insufficient { required }) => {
                        // The provider proved the empty call changed nothing.
                        flight.proved();
                        checked_end(offset, required)?;
                        let mut output = BytesMut::try_zeroed_metered(required, meter)
                            .map_err(|_| crypto_capacity())?;
                        flight.begin_mutation();
                        let (written, changed) =
                            match self.crypto.write_handshake_bounded(&mut output) {
                                Ok(result) => result,
                                Err(_) => {
                                    flight.proved();
                                    return Err(crypto_capacity());
                                }
                            };
                        if written != required {
                            meter.quarantine();
                            return Err(crypto_capacity());
                        }
                        flight.frame = frame::Crypto {
                            offset,
                            data: output.freeze(),
                        };
                        changed
                    }
                    Err(_) => {
                        flight.proved();
                        return Err(crypto_capacity());
                    }
                }
            } else {
                let mut output = Vec::new();
                let changed = self.crypto.write_handshake(&mut output);
                checked_end(offset, output.len())?;
                flight.frame = frame::Crypto {
                    offset,
                    data: Bytes::from(output),
                };
                changed
            };
            #[cfg(test)]
            if bounded {
                self.crypto_barriers[0] += 1;
                self.crypto_barriers[1] += usize::from(changed.is_some());
                self.crypto_barriers[2] +=
                    usize::from(changed.is_some() && flight.frame.data.is_empty());
            }
            let changed_space = match (space, changed.as_ref()) {
                (_, None) => None,
                (SpaceId::Initial, Some(_)) => Some(SpaceId::Handshake),
                (SpaceId::Handshake, Some(_)) => Some(SpaceId::Data),
                _ => {
                    self.retain_failed_crypto(space, flight);
                    return Err(TransportError::INTERNAL_ERROR(
                        "unexpected 1-RTT handshake keys",
                    ));
                }
            };
            // The provider can already have consumed its flight. A broken key
            // barrier is failstop, never a rollback or an unchecked expect.
            let next = if changed_space == Some(SpaceId::Data) {
                match self.crypto.next_1rtt_keys() {
                    Some(next) => Some(next),
                    None => {
                        self.retain_failed_crypto(space, flight);
                        return Err(TransportError::INTERNAL_ERROR(
                            "next handshake keys unavailable",
                        ));
                    }
                }
            } else {
                None
            };
            let length = flight.frame.data.len();
            if let (Some(space), Some(keys)) = (changed_space, changed) {
                debug_assert!(self.spaces[space].crypto.is_none());
                if let Some(next) = next {
                    self.next_crypto = Some(next);
                }
                self.spaces[space].crypto = Some(keys);
                self.highest_space = space;
                if space == SpaceId::Data && self.side.is_client() {
                    self.zero_rtt_crypto = None;
                }
            }
            if length != 0 {
                if let (Some(mut replay), State::Handshake(state)) =
                    (replay.take(), &mut self.state)
                {
                    replay.frame = flight.frame.clone();
                    state.client_hello = Some(replay);
                }
                self.spaces[space].crypto_offset = offset + length as u64;
                self.spaces[space].pending.crypto.push_back(flight);
                trace!("wrote {} {:?} CRYPTO bytes", length, space);
            } else {
                flight.proved();
                if space == self.highest_space {
                    break;
                }
            }
        }
        Ok(())
    }
    /// Commit the already-proved empty output using one real, pre-held Pool
    /// record. Coalesced early packets reuse it synchronously; no late reserve.
    pub(super) fn write_idle_crypto(&mut self, flight: &mut CryptoEntry) -> Result<(), TransportError> {
        flight.begin_mutation();
        match self.crypto.write_handshake_bounded(&mut []) {
            Ok((0, None)) => { flight.proved(); Ok(()) }
            _ => Err(TransportError::INTERNAL_ERROR("prepared idle TLS output changed")),
        }
    }

    fn retain_failed_crypto(&mut self, space: SpaceId, mut flight: CryptoEntry) {
        if let Some(meter) = &self.endpoint_config.receive_backing_meter {
            meter.quarantine();
        }
        if flight.frame.data.is_empty() {
            flight.proved();
        } else {
            self.spaces[space].pending.crypto.push_back(flight);
        }
    }
}
fn checked_end(offset: u64, length: usize) -> Result<u64, TransportError> {
    offset
        .checked_add(length as u64)
        .filter(|end| *end < 1 << 62)
        .ok_or_else(crypto_capacity)
}
fn crypto_capacity() -> TransportError {
    TransportError::INTERNAL_ERROR("CRYPTO output admission refused")
}
