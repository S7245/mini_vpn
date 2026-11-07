//! Original Connection key rotation and authentication effects. Owns no second
//! crypto session or timer; provider, current/next/previous keys remain in the
//! Connection. Entry: update_keys/commit_packet_authentication. Tests: original
//! key_update tests and tests/admission/packet_pressure.
use super::*;

impl Connection {
    /// Apply the original key-state effects only after read-only packet-body
    /// authentication. Future admission must precede this commit and dedup/ACK.
    pub(super) fn commit_packet_authentication(
        &mut self,
        now: Instant,
        space: SpaceId,
        result: packet_crypto::DecryptPacketResult,
    ) -> u64 {
        if result.outgoing_key_update_acked {
            if let Some(prev) = self.prev_crypto.as_mut() {
                prev.end_packet = Some((result.number, now));
                self.set_key_discard_timer(now, space);
            }
        }

        if result.incoming_key_update {
            trace!("key update authenticated");
            self.update_keys(Some((result.number, now)), true);
            self.set_key_discard_timer(now, space);
        }

        result.number
    }

    pub(super) fn update_keys(&mut self, end_packet: Option<(u64, Instant)>, remote: bool) {
        // Preserve the original non-transactional caller's invariant contract.
        // Packet admission uses the Result boundary directly and never this expect.
        self.prepare_key_update()
            .and_then(|prepared| self.install_key_update(prepared, end_packet, remote))
            .expect("only called for `Data` packets");
    }

    pub(super) fn prepare_key_update(&mut self) -> Result<PreparedKeyUpdate, TransportError> {
        let shape = self.key_update_shape().ok_or_else(key_error)?;
        // The provider may advance its secrets before returning (or unwinding).
        // No known-Full refusal is legal from this point onwards.
        let mutation = KeyMutation {
            meter: self.endpoint_config.receive_backing_meter.clone(),
            complete: false,
        };
        let keys = self.crypto.next_1rtt_keys().ok_or_else(key_error)?;
        let phase_size = keys
            .local
            .confidentiality_limit()
            .saturating_sub(KEY_UPDATE_MARGIN);
        Ok(PreparedKeyUpdate {
            owner: self as *const Self as usize,
            shape,
            keys,
            phase_size,
            mutation,
        })
    }

    pub(super) fn install_key_update(
        &mut self,
        mut prepared: PreparedKeyUpdate,
        end_packet: Option<(u64, Instant)>,
        remote: bool,
    ) -> Result<(), TransportError> {
        if prepared.owner != self as *const Self as usize
            || self.key_update_shape() != Some(prepared.shape)
        {
            if let Some(meter) = &self.endpoint_config.receive_backing_meter {
                meter.quarantine();
            }
            return Err(key_error());
        }
        let (Some(current), Some(next)) = (
            self.spaces[SpaceId::Data].crypto.as_mut(),
            self.next_crypto.as_mut(),
        ) else {
            return Err(key_error());
        };
        let old = mem::replace(&mut current.packet, mem::replace(next, prepared.keys));
        self.key_phase_size = prepared.phase_size;
        self.spaces[SpaceId::Data].sent_with_keys = 0;
        self.prev_crypto = Some(PrevCrypto {
            crypto: old,
            end_packet,
            update_unacked: remote,
        });
        self.key_phase = !self.key_phase;
        prepared.mutation.complete = true;
        Ok(())
    }

    fn key_update_shape(&self) -> Option<(bool, usize, usize)> {
        Some((
            self.key_phase,
            &*self.spaces[SpaceId::Data].crypto.as_ref()?.packet.remote as *const _ as *const ()
                as usize,
            &*self.next_crypto.as_ref()?.remote as *const _ as *const () as usize,
        ))
    }
}

pub(super) struct PreparedKeyUpdate {
    owner: usize,
    shape: (bool, usize, usize),
    keys: crypto::KeyPair<Box<dyn crypto::PacketKey>>,
    phase_size: u64,
    mutation: KeyMutation,
}
struct KeyMutation {
    meter: Option<bytes::BackingMeter>,
    complete: bool,
}
impl Drop for KeyMutation {
    fn drop(&mut self) {
        if !self.complete {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
    }
}
fn key_error() -> TransportError {
    TransportError::INTERNAL_ERROR("key update provider or custody failed")
}
