//! Real command entry contract: seven local bytes, actual Node stop, original
//! child wait and bounded pipe EOFs. No TUN, external target or throughput gate.
use mini_vpn::shared::TargetAddr;
use mini_vpn::tuic::{encode_authenticate, encode_connect};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Notify;

#[derive(Default)]
struct Overflow {
    seen: AtomicBool,
    changed: Notify,
}
impl Overflow {
    fn trip(&self) {
        self.seen.store(true, Ordering::Release);
        self.changed.notify_one();
    }
}
async fn bounded(
    mut pipe: impl AsyncRead + Unpin,
    capacity: usize,
    overflow: &Overflow,
) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(capacity);
    let mut scratch = [0; 8192];
    loop {
        let n = match pipe.read(&mut scratch).await {
            Ok(n) => n,
            Err(error) => {
                overflow.trip();
                return Err(error);
            }
        };
        if n == 0 {
            return Ok(bytes);
        }
        let keep = n.min(capacity - bytes.len());
        bytes.extend_from_slice(&scratch[..keep]);
        if keep < n {
            overflow.trip();
        } // Continue bounded drain; original waiter kills.
    }
}
fn server() -> quinn::Endpoint {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(base.join("server-cert.pem")).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(base.join("server-key.pem")).unwrap(),
    ))
    .unwrap()
    .unwrap();
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn actual_binary_sink_uses_saved_node_and_settles_before_success_exit() {
    let mut server = server();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_mini_vpn"))
        .current_dir(root)
        .env_clear()
        .env(
            "MINI_VPN_TUIC_SERVER",
            server.local_addr().unwrap().to_string(),
        )
        .env("MINI_VPN_TUIC_UUID", "00000000-0000-0000-0000-000000000000")
        .env("MINI_VPN_TUIC_PASSWORD", "local-test-only")
        .env("MINI_VPN_TUIC_SNI", "localhost")
        .env("MINI_VPN_TUIC_CA_PATH", root.join("certs/dev/ca-cert.pem"))
        .env("MINI_VPN_TUIC_TCP_POOL", "1")
        .env("MINI_VPN_H10D16_BYTE_OWNED_EGRESS", "1")
        .args(["tuic-tcp-sink-probe", "192.0.2.1:443", "1"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let overflow = Overflow::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let waiter = async {
        let mut forced = false;
        loop {
            tokio::select! {biased;
                result=child.wait()=>break (result,forced),
                _=overflow.changed.notified(),if !forced=>{forced=true;let _=child.start_kill();},
                _=tokio::time::sleep_until(deadline),if !forced=>{forced=true;let _=child.start_kill();},
            }
        }
    };
    let peer = tokio::time::timeout_at(deadline, async {
        let peer = server
            .accept()
            .await
            .ok_or("no incoming")?
            .await
            .map_err(|_| "handshake")?;
        let mut auth = peer.accept_uni().await.map_err(|_| "no AUTH")?;
        let actual = auth.read_to_end(64).await.map_err(|_| "AUTH read")?;
        let mut token = [0; 32];
        peer.export_keying_material(&mut token, &[0; 16], b"local-test-only")
            .map_err(|_| "export")?;
        let auth_correct = actual == encode_authenticate(&[0; 16], &token);
        drop(auth);
        let target = TargetAddr::parse("192.0.2.1:443").map_err(|_| "target")?;
        let header = encode_connect(&target);
        let (mut send, mut recv) = peer.accept_bi().await.map_err(|_| "no CONNECT")?;
        let mut observed = vec![0; header.len()];
        recv.read_exact(&mut observed)
            .await
            .map_err(|_| "CONNECT read")?;
        send.write_all(b"cli-eof").await.map_err(|_| "payload")?;
        send.finish().map_err(|_| "EOF")?;
        let _ = recv.read_to_end(1024).await;
        drop(recv);
        drop(send);
        let actual_stop = matches!(peer.closed().await,quinn::ConnectionError::ApplicationClosed(close) if close.error_code==0u32.into()&&close.reason.as_ref()==b"node_stop");
        Ok::<_, &str>((auth_correct, observed == header, actual_stop))
    });
    let ((status, forced), out, err, peer) = tokio::join!(
        waiter,
        bounded(stdout, 128 * 1024, &overflow),
        bounded(stderr, 64 * 1024, &overflow),
        peer
    );
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"local test cleanup");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(3), drain.drained()).await;
    // All assertions follow original child wait, pipe completion and endpoint drain.
    println!(
        "original_child_status={status:?} forced={forced} stdout_bytes={out:?} stderr_bytes={err:?} original_peer={peer:?} server_drained={drained:?}"
    );
    assert!(drained.unwrap());
    assert!(!forced && !overflow.seen.load(Ordering::Acquire));
    assert!(
        status.unwrap().success(),
        "stderr={:?}",
        err.as_ref().map(|b| String::from_utf8_lossy(b))
    );
    assert_eq!(peer.unwrap().unwrap(), (true, true, true));
    let out = String::from_utf8(out.unwrap()).unwrap();
    assert!(err.unwrap().is_empty());
    let reports = out
        .lines()
        .filter(|line| line.starts_with("tuic_tcp_sink_probe "))
        .collect::<Vec<_>>();
    assert_eq!(reports.len(), 1, "original stdout={out:?}");
    let fields = reports[0].split_whitespace().skip(1).collect::<Vec<_>>();
    for expected in [
        "target=192.0.2.1:443",
        "requested_duration_secs=1",
        "bytes=7",
        "eof=true",
    ] {
        assert_eq!(
            fields.iter().filter(|field| **field == expected).count(),
            1,
            "original stdout={out:?}"
        );
    }
    let reads = fields
        .iter()
        .filter_map(|field| field.strip_prefix("reads="))
        .collect::<Vec<_>>();
    assert_eq!(reads.len(), 1);
    assert!(reads[0].parse::<u64>().unwrap() > 0);
}

#[test]
fn original_public_sink_observation_remains_constructible_from_external_crate() {
    let value = mini_vpn::tuic::probe::Observation {
        phase: mini_vpn::tuic::probe::Phase::Complete,
        sink: None,
        node: None,
    };
    assert_eq!(value.clone(), value);
}
