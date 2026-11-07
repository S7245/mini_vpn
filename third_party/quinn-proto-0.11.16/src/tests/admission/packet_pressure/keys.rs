use super::*;

#[test]
fn incoming_key_full_keeps_original_keys_until_actual_peer_retransmits() {
    recovery(false, PacketShape::KeyUpdate);
}

#[test]
fn incoming_key_full_can_admit_the_original_authenticated_packet_later() {
    recovery(true, PacketShape::KeyUpdate);
}

#[test]
fn repeated_incoming_key_full_preserves_original_deadline() {
    deadline(PacketShape::KeyUpdate);
}

#[test]
fn incoming_key_mixed_datagram_and_stream_refuses_atomically_then_replays_once() {
    recovery(true, PacketShape::KeyUpdateAndDatagram);
}
#[test]
fn incoming_key_mixed_actual_peer_recovers_only_the_reliable_stream() {
    recovery(false, PacketShape::KeyUpdateAndDatagram);
}
#[test]
fn incoming_key_mixed_full_preserves_original_deadline() {
    deadline(PacketShape::KeyUpdateAndDatagram);
}

use crate::crypto::{self, Session};
use std::sync::atomic::{AtomicUsize, Ordering};

struct FaultConfig {
    inner: Arc<dyn crypto::ClientConfig>,
    armed: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl crypto::ClientConfig for FaultConfig {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        name: &str,
        params: &TransportParameters,
    ) -> Result<Box<dyn Session>, ConnectError> {
        Ok(Box::new(FaultSession {
            inner: self.inner.clone().start_session(version, name, params)?,
            armed: self.armed.clone(),
            calls: self.calls.clone(),
        }))
    }
}
struct FaultSession {
    inner: Box<dyn Session>,
    armed: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl Session for FaultSession {
    fn initial_keys(&self, cid: &ConnectionId, side: Side) -> crypto::Keys {
        self.inner.initial_keys(cid, side)
    }
    fn handshake_data(&self) -> Option<Box<dyn std::any::Any>> {
        self.inner.handshake_data()
    }
    fn peer_identity(&self) -> Option<Box<dyn std::any::Any>> {
        self.inner.peer_identity()
    }
    fn early_crypto(&self) -> Option<(Box<dyn crypto::HeaderKey>, Box<dyn crypto::PacketKey>)> {
        self.inner.early_crypto()
    }
    fn early_data_accepted(&self) -> Option<bool> {
        self.inner.early_data_accepted()
    }
    fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }
    fn read_handshake(&mut self, input: &[u8]) -> Result<bool, TransportError> {
        let result = self.inner.read_handshake(input);
        match self.armed.load(Ordering::Relaxed) {
            5 => { self.calls.fetch_add(1, Ordering::Relaxed); Err(TransportError::INTERNAL_ERROR("fault after actual TLS input")) }
            6 => { self.calls.fetch_add(1, Ordering::Relaxed); panic!("fault after actual TLS input"); }
            _ => result,
        }
    }
    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        self.inner.transport_parameters()
    }
    fn write_handshake(&mut self, output: &mut Vec<u8>) -> Option<crypto::Keys> {
        self.inner.write_handshake(output)
    }
    fn write_handshake_bounded(
        &mut self,
        output: &mut [u8],
    ) -> Result<(usize, Option<crypto::Keys>), crypto::HandshakeOutputError> {
        let result = self.inner.write_handshake_bounded(output);
        // Low byte chooses the existing fault; optional high bytes select the
        // actual provider call at which it fires. Full must consume no calls.
        let armed = self.armed.load(Ordering::Relaxed);
        let mode = armed & 0xff;
        if matches!(mode, 3 | 4) {
            let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            if call == (armed >> 8).max(1) {
                if mode == 3 { return Err(crypto::HandshakeOutputError::Unsupported); }
                panic!("explicit fault after actual idle TLS output call");
            }
        }
        result
    }
    fn handshake_output_is_idle(&self) -> Option<bool> { self.inner.handshake_output_is_idle() }
    fn next_1rtt_keys(&mut self) -> Option<crypto::KeyPair<Box<dyn crypto::PacketKey>>> {
        let keys = self.inner.next_1rtt_keys();
        match self.armed.load(Ordering::Relaxed) {
            1 => {
                self.calls.fetch_add(1, Ordering::Relaxed);
                None
            }
            2 => {
                self.calls.fetch_add(1, Ordering::Relaxed);
                panic!("explicit fault after actual rustls next-secret advancement");
            }
            _ => keys,
        }
    }
    fn is_valid_retry(&self, cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        self.inner.is_valid_retry(cid, header, payload)
    }
    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), crypto::ExportKeyingMaterialError> {
        self.inner.export_keying_material(output, label, context)
    }
}

#[test]
fn incoming_key_provider_is_not_consumed_on_full_and_unknown_result_keeps_quarantine() {
    for mode in [1, 2] {
        let armed = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let config = ClientConfig::new(Arc::new(FaultConfig {
            inner: client_config().crypto,
            armed: armed.clone(),
            calls: calls.clone(),
        }));
        let SparsePacket {
            budget,
            mut pair,
            client,
            packet,
            ..
        } = sparse_packet_with_client(PacketShape::KeyUpdate, config);
        armed.store(mode, Ordering::Relaxed);
        let before = pair
            .client_conn_mut(client)
            .packet_admission_snapshot_for_probe();
        let phase = pair.client_conn_mut(client).key_phase_for_packet_probe();
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert!(pair.client.process_one_connection_event(client));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "Full must precede the actual provider"
        );
        assert_eq!(
            pair.client_conn_mut(client)
                .packet_admission_snapshot_for_probe(),
            before
        );
        assert!(!budget.quarantined());
        drop(pressure);
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pair.client.process_one_connection_event(client)
        }));
        assert_eq!(result.is_err(), mode == 2);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            pair.client_conn_mut(client).key_phase_for_packet_probe(),
            phase
        );
        assert!(budget.quarantined() && budget.used() > 0);
        if mode == 1 {
            let first = pair.client_conn_mut(client).retained_error().unwrap();
            assert!(
                matches!(&first, ConnectionError::TransportError(e) if e.reason == "key update provider or custody failed")
            );
            let now = pair.time;
            pair.client_conn_mut(client)
                .close(now, 0u32.into(), Bytes::new());
            assert_eq!(pair.client_conn_mut(client).retained_error(), Some(first));
        }
        let retained = super::super::send_buffer_inventory::retained_layouts(&pair);
        drop((packet, pair));
        retained.assert_quarantined(&budget);
    }
}

pub(super) fn fault_config() -> (ClientConfig, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let armed = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let config = ClientConfig::new(Arc::new(FaultConfig {
        inner: client_config().crypto, armed: armed.clone(), calls: calls.clone(),
    }));
    (config, armed, calls)
}
