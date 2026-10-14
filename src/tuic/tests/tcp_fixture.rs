//! Shared actual loopback Quinn fixture; no protocol simulation.
use super::*;

pub(crate) struct UdpPair {
    pub(crate) server: Endpoint,
    pub(crate) client: Endpoint,
    pub(crate) connection: Connection,
    pub(crate) peer: Connection,
}

impl UdpPair {
    pub(crate) async fn new(uni_streams: u32, stream_window: u32) -> Self {
        Self::with_limits(uni_streams, stream_window, 100).await
    }

    pub(crate) async fn with_limits(uni_streams: u32, stream_window: u32, bi_streams: u32) -> Self {
        let (server, client, address) = d16_quinn_test_endpoints();
        let config = Self::server_config(uni_streams, stream_window, bi_streams, false);
        server.set_server_config(Some(config));
        let (connection, peer) = tokio::join!(
            async { client.connect(address, "localhost").unwrap().await.unwrap() },
            async { server.accept().await.unwrap().await.unwrap() },
        );
        Self {
            server,
            client,
            connection,
            peer,
        }
    }

    pub(crate) fn server_config(
        uni_streams: u32,
        stream_window: u32,
        bi_streams: u32,
        alpn: bool,
    ) -> quinn::ServerConfig {
        let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
        let mut cert_reader =
            std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-cert.pem")).unwrap());
        let certs = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut key_reader =
            std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-key.pem")).unwrap());
        let key = rustls_pemfile::private_key(&mut key_reader)
            .unwrap()
            .unwrap();
        let mut config = if alpn {
            let mut crypto = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap();
            crypto.alpn_protocols = vec![b"h3".to_vec()];
            quinn::ServerConfig::with_crypto(Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(crypto).unwrap(),
            ))
        } else {
            quinn::ServerConfig::with_single_cert(certs, key).unwrap()
        };
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_uni_streams(uni_streams.into());
        transport.max_concurrent_bidi_streams(bi_streams.into());
        transport.stream_receive_window(stream_window.into());
        config.transport_config(Arc::new(transport));
        config
    }

    pub(crate) async fn upstream(&self, mode: UdpRelayMode) -> TuicUpstream {
        let clock = Instant::now();
        let slot = Arc::new(TcpPoolGenerationSlot::new(
            0,
            self.connection.clone(),
            1,
            1,
            clock,
        ));
        let admission =
            TcpPoolAdmission::with_current_generation_activity(vec![slot.current_activity().await]);
        TuicUpstream {
            relay_policy: Arc::new(RelayFamilyPolicy::default()),
            endpoint: self.client.clone(),
            udp_send_service_policy: quic::QuicUdpSendServicePolicy::QuinnDefault,
            udp_send_service_stats: None,
            udp_runtime: Arc::new(node_runtime::UdpRuntime::default()),
            server: self.server.local_addr().unwrap(),
            sni: "localhost".into(),
            uuid: [0; 16],
            password: String::new(),
            tcp_pool_slots: vec![slot],
            tcp_pool_draining_predecessor: Arc::new(AtomicBool::new(false)),
            tcp_pool_admission: admission,
            udp_drops: AtomicU64::new(0),
            udp_stream_sender: Default::default(),
            udp_stream_fallbacks: AtomicU64::new(0),
            last_udp_activity: AtomicU64::new(0),
            clock,
            zero_rtt: false,
            quic_stats_secs: None,
            quic_stats_stop: None,
            udp_relay_mode: mode,
            metrics: Arc::new(Metrics::default()),
        }
    }

    pub(crate) async fn close(self) {
        self.connection.close(0u32.into(), b"test complete");
        self.peer.close(0u32.into(), b"test complete");
        self.client.close(0u32.into(), b"test complete");
        self.server.close(0u32.into(), b"test complete");
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(self.client.wait_idle(), self.server.wait_idle());
        })
        .await
        .expect("loopback endpoints must settle");
    }

    pub(crate) async fn assert_bi_progress(&self) {
        tokio::time::timeout(Duration::from_secs(2), async {
            let (client, server) = tokio::join!(
                async {
                    let (mut send, mut recv) = self.connection.open_bi().await.unwrap();
                    send.write_all(b"m").await.unwrap();
                    send.finish().unwrap();
                    recv.read_to_end(1).await.unwrap()
                },
                async {
                    let (mut send, mut recv) = self.peer.accept_bi().await.unwrap();
                    let marker = recv.read_to_end(1).await.unwrap();
                    send.write_all(&marker).await.unwrap();
                    send.finish().unwrap();
                    marker
                },
            );
            assert_eq!(client, b"m");
            assert_eq!(server, b"m");
        })
        .await
        .expect("an unrelated stream must still progress after UDP pressure");
    }
}

pub(crate) fn upstream_endpoint(upstream: &TuicUpstream) -> Endpoint {
    upstream.endpoint.clone()
}

/// Passive current-generation transport observation; never reconnects or reserves.
pub(crate) fn upstream_ready_connection(upstream: &TuicUpstream) -> Option<Connection> {
    let state = upstream.tcp_pool_slots.first()?.state.try_lock().ok()?;
    Some(state.current.transport.clone())
}

/// Hold the actual registration lock through a caller's complete cancellation.
/// No recovery sampler may run: it uses the blocking side of this mutex.
pub(crate) async fn with_ready_registry_held<F: std::future::Future>(
    upstream: &TuicUpstream,
    work: F,
) -> F::Output {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::task::Poll;

    assert!(!upstream.udp_runtime.monitor_started.load(Ordering::Acquire));
    let pressure = {
        let state = upstream.tcp_pool_slots[0].state.lock().await;
        state.current.write_pressure.clone()
    };
    let guard = pressure.writers.lock().unwrap();
    assert!(guard.is_empty());
    let next = pressure.next_writer.load(Ordering::Relaxed);
    let mut work = Box::pin(work);
    // Catch while holding the guard so a failed assertion cannot poison it.
    let result = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    })
    .await;
    let dropped = catch_unwind(AssertUnwindSafe(|| drop(work)));
    drop(guard);
    let unchanged = pressure.writers.lock().unwrap().is_empty()
        && pressure.next_writer.load(Ordering::Relaxed) == next;
    let value = result.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    dropped.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(
        unchanged,
        "contention must never register or allocate a writer"
    );
    assert!(!upstream.udp_runtime.monitor_started.load(Ordering::Acquire));
    value
}
