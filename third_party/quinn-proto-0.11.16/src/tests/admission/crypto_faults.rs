//! Explicit provider-contract fault injection around real rustls, never a
//! naturally reachable TLS claim. The original Connection remains the owner.
use super::*;
use crate::crypto::{self, Session};
use std::sync::atomic::{AtomicBool, Ordering};

struct FaultConfig {
    inner: Arc<dyn crypto::ClientConfig>,
    panic: bool,
    hit: Arc<AtomicBool>,
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
            panic: self.panic,
            hit: self.hit.clone(),
        }))
    }
}
struct FaultSession {
    inner: Box<dyn Session>,
    panic: bool,
    hit: Arc<AtomicBool>,
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
        self.inner.read_handshake(input)
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
        let result = self.inner.write_handshake_bounded(output)?;
        if self.panic && result.1.is_some() {
            self.hit.store(true, Ordering::Relaxed);
            panic!("injected interruption after original provider key transfer");
        }
        Ok(result)
    }
    fn next_1rtt_keys(&mut self) -> Option<crypto::KeyPair<Box<dyn crypto::PacketKey>>> {
        // Consume the real barrier first; model a broken provider's absent result.
        let _keys = self.inner.next_1rtt_keys();
        self.hit.store(true, Ordering::Relaxed);
        None
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
fn crypto_provider_key_fault_and_unwind_remain_charged_quarantine() {
    for panic in [false, true] {
        let budget = cold_fixture_budget(2);
        let mut config = admission_endpoint_config();
        config.receive_datagram_limit(1472).unwrap();
        config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
        let mut pair = Pair::new(Arc::new(config), server_config());
        let hit = Arc::new(AtomicBool::new(false));
        let client = pair.begin_connect(ClientConfig::new(Arc::new(FaultConfig {
            inner: client_config().crypto,
            panic,
            hit: hit.clone(),
        })));
        pair.drive_client();
        pair.drive_server();
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let now = pair.time;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pair.client.process_connection_events(client);
        }));
        assert!(hit.load(Ordering::Relaxed));
        assert_eq!(result.is_err(), panic);
        assert!(budget.quarantined() && budget.used() > 0);
        if !panic {
            let conn = pair.client_conn_mut(client);
            let first = conn.retained_error().unwrap();
            assert!(matches!(&first, ConnectionError::TransportError(error)
                if error.reason == "next handshake keys unavailable"));
            assert!(conn.is_closed() && !conn.is_drained());
            assert_eq!(
                conn.poll_timeout(),
                Some(conn.original_close_deadline_for_stream_probe(now))
            );
            conn.close(now, 0u32.into(), Bytes::new());
            assert_eq!(conn.retained_error(), Some(first));
        }
        // Proto destruction is known. Q does not become a fake driver JOIN.
        drop(pair);
        assert_eq!(budget.used(), 1);
        assert!(budget.quarantined());
    }
}

#[test]
fn crypto_foreign_requeue_at_original_pto_retains_first_and_quarantine() {
    let budget = cold_fixture_budget(4);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let client = pair.begin_connect(client_config());
    let other = pair.begin_connect(client_config());
    pair.client.drive_outgoing(pair.time);
    assert!(!pair.client.outbound.is_empty());
    pair.client.outbound.clear();
    let [Some(left), Some(right)] = pair.client.connections.get_disjoint_mut([&client, &other])
    else {
        panic!("two original connections remain live");
    };
    assert!(left.swap_sent_crypto_for_probe(right)); // Explicit foreign-custody fault.
    let now = left.loss_detection_deadline().unwrap();
    let before = left.crypto_snapshot_for_probe();
    let sent = left.stats().frame_tx.crypto;
    let flight = left.bytes_in_flight();
    left.handle_timeout(now);
    let deadline = left.original_close_deadline_for_stream_probe(now);
    let mut wire = Vec::new();
    assert!(left.poll_transmit(now, 1, &mut wire).is_none());
    assert!(wire.is_empty());
    assert_eq!(left.crypto_snapshot_for_probe().pn, before.pn);
    assert_eq!(left.stats().frame_tx.crypto, sent);
    assert_eq!(left.bytes_in_flight(), flight);
    let first = left.retained_error().unwrap();
    assert!(
        matches!(&first, ConnectionError::TransportError(error) if error.reason == "foreign CRYPTO custody")
    );
    assert!(left.is_closed() && !left.is_drained());
    assert_eq!(left.poll_timeout(), Some(deadline));
    left.close(now, 0u32.into(), Bytes::new());
    assert_eq!(left.retained_error(), Some(first));
    assert!(budget.quarantined());
    drop(pair);
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
}
