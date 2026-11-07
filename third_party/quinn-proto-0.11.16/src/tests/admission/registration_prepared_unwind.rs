//! Opaque constructor and destructor interruptions around actual rustls sessions.
use super::*;
use crate::crypto::{self, Session};
struct DropConfig {
    inner: Arc<dyn crypto::ClientConfig>,
    payload: Arc<()>,
    constructor: bool,
}
impl crypto::ClientConfig for DropConfig {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        name: &str,
        params: &TransportParameters,
    ) -> Result<Box<dyn Session>, ConnectError> {
        if self.constructor {
            std::panic::panic_any(self.payload.clone());
        }
        Ok(Box::new(DropSession {
            inner: self.inner.clone().start_session(version, name, params)?,
            payload: self.payload.clone(),
        }))
    }
}
struct DropSession {
    inner: Box<dyn Session>,
    payload: Arc<()>,
}
impl Drop for DropSession {
    fn drop(&mut self) {
        std::panic::panic_any(self.payload.clone());
    }
}
impl Session for DropSession {
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
        self.inner.write_handshake_bounded(output)
    }

    fn next_1rtt_keys(&mut self) -> Option<crypto::KeyPair<Box<dyn crypto::PacketKey>>> {
        self.inner.next_1rtt_keys()
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
fn original_opaque_panic_never_mints_clean_disposal_or_constructor_success() {
    for constructor in [false, true] {
        let budget = cold_fixture_budget(2);
        let mut config = admission_endpoint_config();
        config.receive_datagram_limit(1472).unwrap();
        config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
        config.endpoint_pacing_service(Some(EndpointPacingServiceConfig::endpoint_window_v1()));
        let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, None).unwrap();
        let payload = Arc::new(());
        let mut client = client_config();
        client.crypto = Arc::new(DropConfig {
            inner: client.crypto.clone(),
            payload: payload.clone(),
            constructor,
        });
        let panic = if constructor {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = endpoint.prepare_connect(
                    Instant::now(),
                    client,
                    "[::1]:4433".parse().unwrap(),
                    "localhost",
                );
            }))
            .unwrap_err()
        } else {
            let (ticket, conn) = endpoint
                .prepare_connect(
                    Instant::now(),
                    client,
                    "[::1]:4433".parse().unwrap(),
                    "localhost",
                )
                .unwrap();
            let panic =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(conn))).unwrap_err();
            assert!(budget.quarantined());
            assert!(!endpoint.cancel_connect(ticket));
            panic
        };
        assert!(Arc::ptr_eq(&panic.downcast::<Arc<()>>().unwrap(), &payload));
        assert!(budget.quarantined());
        assert_eq!(endpoint.open_connections(), 0);
        drop(endpoint);
        assert_eq!(budget.used(), 1);
    }
}
