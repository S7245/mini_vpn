//! D12 tracer: a real Proto whole-packet admission refusal (shared budget
//! Full inside quinn-proto) on one Node connection failstops exactly that
//! connection with a typed local close; the sibling connection, the shared
//! budget (non-Q) and the Node stay live. Real loopback QUIC/TUIC AUTH, the
//! original NodeHost/monitor; the only stimulus is an external budget squeeze.
use super::super::*;
use super::running::boot;
use super::startup::{config, finish, guarded};
use crate::tuic::node::admission_failstop::NODE_ADMISSION_FAILSTOP_REASON;

/// In-memory test identity; only its public CA is written for the unchanged
/// production `ca_path` loader (certs/dev/server-key.pem is absent, D9).
struct Identity {
    dir: std::path::PathBuf,
    server: quinn::ServerConfig,
}
impl Identity {
    fn new() -> Self {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "d12-failstop-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("ca.pem"), key.cert.pem()).unwrap();
        let cert = rustls::pki_types::CertificateDer::from(key.cert.der().to_vec());
        let private = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()),
        );
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], private)
            .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_uni_streams(8u32.into());
        transport.max_concurrent_bidi_streams(100u32.into());
        server.transport_config(Arc::new(transport));
        Self { dir, server }
    }
}
impl Drop for Identity {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.dir.join("ca.pem"));
        let _ = std::fs::remove_dir(&self.dir);
    }
}

async fn transport(data: &TuicUpstream, index: usize) -> Connection {
    data.tcp_pool_slots[index].state.lock().await.current.transport.clone()
}

#[tokio::test(flavor = "current_thread")]
async fn proto_admission_refusal_failstops_only_that_node_connection() {
    let identity = Identity::new();
    let server = Endpoint::server(identity.server.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.dir.join("ca.pem").to_str().unwrap().to_owned();
    cfg.tcp_pool = 2;
    cfg.relay_policy = Arc::new(RelayFamilyPolicy::from_sources(
        crate::upstream::RelayFamilyPolicySources {
            tuic_native_chunk: Some("1"),
            ..Default::default()
        },
    ));
    let budget = bytes::BackingBudget::new(16 << 20).unwrap();
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    host.cold_meter_for_test(bytes::BackingMeter::with_budget(&budget));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let (victim, sibling) = (transport(&data, 0).await, transport(&data, 1).await);
        assert_ne!(victim.stable_id(), sibling.stable_id());
        assert_eq!(victim.stats().udp_rx.admission_refused, 0);
        let peers = &peers;
        let budget = &budget;
        let scenario = async {
        // Calibrate: leave a slack big enough for the endpoint's datagram copy
        // but too small for Proto's packet preparation. The external hold is
        // the only stimulus; dropping it restores the full budget.
        // Let post-handshake control traffic (tickets/CIDs) settle first so the
        // squeeze only meets the stimulated connection's packets.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut refused_at = None;
        for slack in (0usize..=16 << 10).step_by(128) {
            let endpoint_before = data.endpoint.stats().receive_backing_rejected_datagrams;
            let hold = budget
                .try_reserve(budget.limit().saturating_sub(budget.used()).saturating_sub(slack))
                .unwrap();
            let (mut send, _recv) = peers[0].open_bi().await.unwrap();
            let _ = send.write_all(&[7u8; 2 << 10]).await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            let refused = victim.stats().udp_rx.admission_refused;
            let endpoint = data.endpoint.stats().receive_backing_rejected_datagrams - endpoint_before;
            drop(hold);
            if refused > 0 || endpoint > 0 {
                eprintln!(
                    "[D12-calibrate] slack={slack} proto_refused={refused} endpoint_rejected={endpoint} sibling_refused={} Q={}",
                    sibling.stats().udp_rx.admission_refused,
                    budget.quarantined()
                );
            }
            if refused > 0 {
                refused_at = Some(slack);
                break;
            }
        }
        assert!(refused_at.is_some(), "no squeeze produced a real Proto admission refusal");
        assert!(!budget.quarantined(), "Proto refusal must stay exact (non-Q)");
        // D12: the Node owner failstops exactly the refused set. The squeeze is
        // budget-wide, so any connection that received during it may also be
        // refused; policy = closed iff refused, never a bystander.
        let wait_closed = |conn: Connection| async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if let Some(reason) = conn.close_reason() {
                        break reason;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("Node owner did not failstop a refused connection")
        };
        let mut live = 0usize;
        for (conn, peer) in [(&victim, &peers[0]), (&sibling, &peers[1])] {
            if conn.stats().udp_rx.admission_refused == 0 {
                tokio::time::sleep(Duration::from_millis(600)).await; // > 2 monitor ticks
                assert!(conn.close_reason().is_none(), "unrefused connection was closed");
                assert!(peer.close_reason().is_none());
                assert_eq!(conn.stats().udp_rx.admission_refused, 0);
                live += 1;
                continue;
            }
            assert_eq!(wait_closed(conn.clone()).await, quinn::ConnectionError::LocallyClosed);
            match tokio::time::timeout(Duration::from_secs(2), peer.closed()).await.unwrap() {
                quinn::ConnectionError::ApplicationClosed(close) => {
                    assert_eq!(&close.reason[..], NODE_ADMISSION_FAILSTOP_REASON);
                }
                other => panic!("peer saw untyped close: {other:?}"),
            }
        }
        assert!(victim.close_reason().is_some(), "the stimulated connection was refused");
        assert_eq!(live, 1, "the unstimulated sibling must stay live (rerun if squeeze hit it)");
        assert!(!budget.quarantined());
        };
        // Production drives the endpoint monitor through the original NodeHost::run.
        tokio::select! {
            biased;
            () = scenario => {},
            report = host.run() => panic!("original Node run ended during failstop: {report:?}"),
        }
        assert!(host.resources.state().first.is_none(), "connection failstop is not a Node failure");
        assert!(!host.resources.state().quarantined);
        drop(data);
    })
    .await;
    finish(host, server, peers, result).await;
}
