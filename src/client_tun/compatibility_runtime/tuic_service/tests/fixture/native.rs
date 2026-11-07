//! Shared test-only TLS identity, public trust anchor and explicit input profile.
//! Owns no production source, transport, budget or cleanup authority. Private
//! keys stay in memory; temporary public CA uses the unchanged production loader.
//! Entries PublicCa/ServiceProfile construct real original Service inputs;
//! characterization: native_complete, then original loaded M scenarios.
use super::*;

/// Private key stays in memory; only this new fixture's public trust anchor is
/// written for the unchanged production ca_path loader. Old certs are untouched.
pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) struct PublicCa {
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) path: std::path::PathBuf,
    identity: rcgen::CertifiedKey<rcgen::KeyPair>,
}
impl PublicCa {
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let path = std::env::temp_dir().join(format!(
            "n1-native-service-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let result = Self { path, identity };
        std::fs::write(result.path.join("ca.pem"), result.identity.cert.pem()).unwrap();
        result
    }
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn server_config(
        &self,
    ) -> quinn::ServerConfig {
        self.server_config_with_alpn(b"h3")
    }
    /// Same in-memory identity; a foreign ALPN makes the real handshake refuse.
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn server_config_with_alpn(
        &self,
        alpn: &[u8],
    ) -> quinn::ServerConfig {
        let cert = rustls::pki_types::CertificateDer::from(self.identity.cert.der().to_vec());
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(self.identity.signing_key.serialize_der()),
        );
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        tls.alpn_protocols = vec![alpn.to_vec()];
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_uni_streams(8u32.into());
        transport.max_concurrent_bidi_streams(100u32.into());
        transport.stream_receive_window(65536u32.into());
        config.transport_config(Arc::new(transport));
        config
    }
}
impl Drop for PublicCa {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.join("ca.pem"));
        let _ = std::fs::remove_dir(&self.path);
    }
}

/// Test input profile only. Original Service/Node/Session retain every runtime
/// authority. Legacy creates the same provided D before the saved factory;
/// Hard creates its actual Native D inside that factory with the original meter.
pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) enum ServiceProfile {
    Legacy,
    Hard(PublicCa),
}
enum DeviceInput {
    Provided(LoopbackTunDevice),
    Native(PacketLink, PacketLink),
}
impl ServiceProfile {
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn new(
        hard: bool,
    ) -> Self {
        if hard {
            Self::Hard(PublicCa::new())
        } else {
            Self::Legacy
        }
    }
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn server_config(
        &self,
    ) -> quinn::ServerConfig {
        match self {
            Self::Legacy => UdpPair::server_config(8, 65536, 100, true),
            Self::Hard(identity) => identity.server_config(),
        }
    }
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn service(
        &self,
        inbound: PacketLink,
        outbound: PacketLink,
        cfg: TuicClientConfig,
        runtime: TunRuntimeConfig,
    ) -> Service<impl Future<Output = Completion> + use<>> {
        self.service_with_partial_probe(inbound, outbound, cfg, runtime, None)
    }
    pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn service_with_partial_probe(
        &self,
        inbound: PacketLink,
        outbound: PacketLink,
        mut cfg: TuicClientConfig,
        mut runtime: TunRuntimeConfig,
        mut partial: Option<Arc<crate::harness::PartialFlushProbe>>,
    ) -> Service<impl Future<Output = Completion> + use<>> {
        let input = match self {
            Self::Legacy => {
                let device = LoopbackTunDevice::new(inbound, outbound);
                DeviceInput::Provided(match partial.take() {
                    Some(probe) => device.with_test_partial_flush_probe(probe),
                    None => device,
                })
            }
            Self::Hard(identity) => {
                cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
                // Protocol tier from the installed shape (C6/D83); equals native_hard for config().
                let reserve = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_reserve_for(&cfg.transport_shape());
                runtime = runtime.with_native_limits((32 << 20) + reserve, 64).unwrap();
                runtime.tun_mtu = 1500;
                DeviceInput::Native(inbound, outbound)
            }
        };
        prepare_factory(
            move |meter: Option<bytes::BackingMeter>| async move {
                match input {
                    DeviceInput::Provided(device) => Ok(device),
                    DeviceInput::Native(inbound, outbound) => {
                        let meter = meter.ok_or_else(|| {
                            ClientError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "hard fixture lost original factory meter",
                            ))
                        })?;
                        let device =
                            LoopbackTunDevice::native_with_factory(64, 1500, &meter, || {
                                Ok((inbound, outbound))
                            })?;
                        Ok::<_, ClientError>(match partial {
                            Some(probe) => device.with_test_partial_flush_probe(probe),
                            None => device,
                        })
                    }
                }
            },
            Configuration {
                tuic: cfg,
                composition: (),
            },
            runtime,
            Arc::new(Metrics::default()),
            NoopSink,
        )
    }
}

// Hard Main may return its original admission refusal after the old Flow limit
// commits Q. This is a control diagnostic, never an invented underlying IO.
pub(in crate::client_tun::compatibility_runtime::tuic_service::tests) fn hard_q_refusal(
    owner: &Arc<SessionControl>,
    saved: &Control,
    parent_stop: bool,
) -> Option<(usize, String, Vec<u8>)> {
    let current = saved
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    assert!(Arc::ptr_eq(&current, owner));
    let retained = {
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref()?;
        assert!(ledger.quarantined && saved.backing_budget.as_ref().unwrap().quarantined());
        let facts = first.facts();
        assert_eq!(
            facts.origin.phase,
            crate::shared::errors::FailurePhase::DeviceWrite
        );
        assert_eq!(
            facts.origin.resource,
            crate::shared::errors::FailureResource::Session
        );
        assert_eq!(facts.class, crate::shared::errors::FailureClass::Io);
        assert_eq!(facts.io_kind, Some(std::io::ErrorKind::Other));
        assert_eq!(facts.raw_os_code, None);
        assert!(facts.opaque && facts.source_content_omitted);
        (
            first as *const _ as usize,
            format!("{facts:?}"),
            first.text_bytes().to_vec(),
        )
    };
    let contracts = owner.flow_contracts();
    assert_eq!(contracts.len(), 1);
    let (deadline, q, reason) = owner.flow_original_stop_for_test(contracts[0].id).unwrap();
    let deadline = deadline.expect("original Flow must already be closing");
    assert!(q && tokio::time::Instant::now() >= deadline);
    assert_eq!(
        reason,
        if parent_stop {
            ("root", "session_stop")
        } else {
            ("local", "close_requested")
        }
    );
    Some(retained)
}
