//! A2 measurement (STATUS step 3): amplification of the real Node QUIC stack
//! under adversarial peer layouts, measured with the A3 counting allocator.
//!
//! The receiver is the production Node endpoint (`client_endpoint_with_udp_send_service_metered`
//! + `client_quic_config_alpn_shaped` with `TransportShape::tuic`) on a
//! protocol-tier meter. The adversary (quinn server + impairing UDP relay)
//! runs in a child process re-executing this test binary, so the
//! process-wide counter observes only the receiver. Kernel socket buffers are
//! not heap and stay under the A2 instance bound. Run alone:
//! `--exact quic::honest_bound::tests::amplification::measure_amplification --ignored --test-threads=1 --nocapture`.
use super::counting_alloc::{live, peak, reset_peak};
use super::*;
use crate::quic::{
    client_endpoint_with_udp_send_service_metered, client_quic_config_alpn_shaped, CcChoice,
    QuicGsoPolicy, QuicPacingPolicy, QuicUdpSendServicePolicy,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

const CHILD: &str = "quic::honest_bound::tests::amplification::adversary_child";
const ENV_SCENARIO: &str = "MINI_VPN_A2_ADV_SCENARIO";
const ENV_DIR: &str = "MINI_VPN_A2_ADV_DIR";
const MIB: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    /// Handshake + idle: c_conn.
    Idle,
    /// Honest fill of the connection window (5 uni × 9MiB, 64KiB writes).
    Bulk,
    /// 4096 uni + 512 bidi peer streams, 1-byte round-robin writes: many
    /// small frames share each datagram.
    SmallFrames,
    /// One 1-byte frame per datagram, round-robin over all peer streams:
    /// worst retained-allocation per buffered byte before defragmentation.
    SparseFrames,
    /// Bulk with downlink reversed in 32-packet batches.
    Reorder,
    /// Bulk with every downlink packet delivered 3×.
    Duplicate,
    /// Bulk with every fourth downlink packet dropped (gap layout + retransmits).
    Gaps,
    /// Bulk while the relay rebinds its server-facing port every 50ms
    /// (path validation + CID issue/retire churn).
    Rebind,
    /// Max-size QUIC datagrams the receiver never reads.
    Datagrams,
    /// Zero-length QUIC datagrams the receiver never reads.
    ZeroDatagrams,
    /// Receiver writes 64B chunks while every downlink packet is dropped
    /// (no ACK: send buffer fills, PTO/retransmit storm).
    TxStormSmall,
    /// Same with 64KiB chunks (per-chunk overhead control).
    TxStormBulk,
}

const ALL: [Scenario; 12] = [
    Scenario::Idle,
    Scenario::Bulk,
    Scenario::SmallFrames,
    Scenario::SparseFrames,
    Scenario::Reorder,
    Scenario::Duplicate,
    Scenario::Gaps,
    Scenario::Rebind,
    Scenario::Datagrams,
    Scenario::ZeroDatagrams,
    Scenario::TxStormSmall,
    Scenario::TxStormBulk,
];

impl Scenario {
    fn name(self) -> String {
        format!("{self:?}")
    }
    fn parse(name: &str) -> Option<Self> {
        ALL.into_iter().find(|s| s.name() == name)
    }
    fn bulk(self) -> bool {
        matches!(
            self,
            Self::Bulk | Self::Reorder | Self::Duplicate | Self::Gaps | Self::Rebind
        )
    }
    /// Honest load the Route-2 preflight must cover; the rest are adversarial
    /// layouts (constant-bounded and metered, failstop on protocol Full).
    fn honest(self) -> bool {
        !matches!(
            self,
            Self::SmallFrames | Self::SparseFrames | Self::ZeroDatagrams | Self::TxStormSmall
        )
    }
    fn tx(self) -> Option<usize> {
        match self {
            Self::TxStormSmall => Some(64),
            Self::TxStormBulk => Some(64 << 10),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------- adversary

/// Child-process entry; returns immediately unless spawned by the parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "A2 adversary child: spawned by measure_amplification"]
async fn adversary_child() {
    let (Ok(name), Ok(dir)) = (std::env::var(ENV_SCENARIO), std::env::var(ENV_DIR)) else {
        return;
    };
    let scenario = Scenario::parse(&name).expect("known scenario");
    let dir = PathBuf::from(dir);
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
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
    transport.max_concurrent_bidi_streams(4096u32.into());
    transport.max_concurrent_uni_streams(16u32.into());
    transport.stream_receive_window((64u32 << 20).into());
    transport.receive_window((64u32 << 20).into());
    transport.send_window(64 << 20);
    transport.datagram_send_buffer_size(4 * MIB);
    server.transport_config(Arc::new(transport));
    let endpoint = quinn::Endpoint::server(server, "127.0.0.1:0".parse().unwrap()).unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let front_addr = front.local_addr().unwrap();
    tokio::spawn(relay(
        scenario,
        front,
        endpoint.local_addr().unwrap(),
        armed.clone(),
    ));
    let tmp = dir.join("addr.tmp");
    std::fs::write(&tmp, front_addr.to_string()).unwrap();
    std::fs::rename(tmp, dir.join("addr")).unwrap();

    let conn = endpoint.accept().await.unwrap().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await; // tickets/CIDs settle
    armed.store(true, Relaxed);
    let written = drive(scenario, &conn).await;
    std::fs::write(dir.join("done"), written.to_string()).unwrap();
    tokio::time::sleep(Duration::from_secs(600)).await; // parent kills us
}

/// Peer send behaviour; returns payload bytes the peer's API accepted.
async fn drive(scenario: Scenario, conn: &quinn::Connection) -> u64 {
    let written = Arc::new(AtomicU64::new(0));
    if scenario.bulk() {
        for _ in 0..5 {
            let mut send = conn.open_uni().await.unwrap();
            let written = written.clone();
            tokio::spawn(async move {
                let chunk = vec![0x5au8; 64 << 10];
                for _ in 0..(9 * MIB / chunk.len()) {
                    if send.write_all(&chunk).await.is_err() {
                        return;
                    }
                    written.fetch_add(chunk.len() as u64, Relaxed);
                }
            });
        }
        stall(&written, Duration::from_millis(1500), Duration::from_secs(25)).await;
    }
    match scenario {
        Scenario::SmallFrames | Scenario::SparseFrames => {
            let mut streams = Vec::new();
            for _ in 0..4096 {
                streams.push(conn.open_uni().await.unwrap());
            }
            for _ in 0..512 {
                streams.push(conn.open_bi().await.unwrap().0);
            }
            let sparse = scenario == Scenario::SparseFrames;
            let deadline = Instant::now() + Duration::from_secs(20);
            'rounds: loop {
                for send in &mut streams {
                    if Instant::now() > deadline {
                        break 'rounds;
                    }
                    let before = conn.stats().udp_tx.datagrams;
                    match tokio::time::timeout(Duration::from_millis(500), send.write(&[1])).await {
                        Ok(Ok(n)) => written.fetch_add(n as u64, Relaxed),
                        _ => break 'rounds, // flow control closed or peer stopped
                    };
                    if sparse {
                        let wait = Instant::now() + Duration::from_millis(5);
                        while conn.stats().udp_tx.datagrams == before && Instant::now() < wait {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
        }
        Scenario::Datagrams | Scenario::ZeroDatagrams => {
            let size = if scenario == Scenario::Datagrams {
                // Headroom so a datagram also fits beside an ACK frame.
                conn.max_datagram_size().unwrap_or(1200).saturating_sub(32)
            } else {
                0
            };
            let mut payload = bytes::Bytes::from(vec![7u8; size]);
            let deadline = Instant::now() + Duration::from_secs(8);
            let (mut sent, mut errors) = (0u64, 0u64);
            while Instant::now() < deadline {
                // Non-blocking: the sender's own queue drops its oldest. A
                // path-MTU change shrinks the limit: resize and keep sending.
                if conn.send_datagram(payload.clone()).is_err() {
                    errors += 1;
                    if conn.close_reason().is_some() {
                        break;
                    }
                    let fit = conn.max_datagram_size().unwrap_or(0).saturating_sub(32);
                    payload = bytes::Bytes::from(vec![7u8; fit.min(size)]);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
                sent += 1;
                if sent % 64 == 0 {
                    tokio::task::yield_now().await;
                }
                written.fetch_add(payload.len() as u64, Relaxed);
            }
            eprintln!("[A2-child] {scenario:?} datagrams_sent={sent} errors={errors} size={size}");
        }
        Scenario::TxStormSmall | Scenario::TxStormBulk => {
            // The receiver writes; keep its streams accepted but unread.
            let accept = conn.clone();
            tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok(pair) = accept.accept_bi().await {
                    held.push(pair);
                }
            });
        }
        _ => {}
    }
    written.load(Relaxed)
}

async fn stall(progress: &AtomicU64, quiet: Duration, cap: Duration) {
    let end = Instant::now() + cap;
    let (mut last, mut since) = (progress.load(Relaxed), Instant::now());
    while Instant::now() < end {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = progress.load(Relaxed);
        if now != last {
            (last, since) = (now, Instant::now());
        } else if since.elapsed() >= quiet {
            return;
        }
    }
}

/// UDP relay: client <-> front <-> upstream <-> server. Impairment applies to
/// the downlink (toward the measured receiver) once armed.
async fn relay(scenario: Scenario, front: Arc<UdpSocket>, server: std::net::SocketAddr, armed: Arc<AtomicBool>) {
    let client = Arc::new(tokio::sync::OnceCell::<std::net::SocketAddr>::new());
    let current = Arc::new(std::sync::Mutex::new(new_upstream(server).await));
    spawn_downlink(scenario, current.lock().unwrap().clone(), front.clone(), client.clone(), armed.clone());
    if scenario == Scenario::Rebind {
        let (current, front, client, armed) = (current.clone(), front.clone(), client.clone(), armed.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if !armed.load(Relaxed) {
                    continue;
                }
                let up = new_upstream(server).await;
                spawn_downlink(Scenario::Idle, up.clone(), front.clone(), client.clone(), armed.clone());
                *current.lock().unwrap() = up;
            }
        });
    }
    let mut buf = vec![0u8; 65536];
    loop {
        let Ok((n, from)) = front.recv_from(&mut buf).await else { return };
        let _ = client.set(from);
        let up = current.lock().unwrap().clone();
        let _ = up.send(&buf[..n]).await;
    }
}

async fn new_upstream(server: std::net::SocketAddr) -> Arc<UdpSocket> {
    let up = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    up.connect(server).await.unwrap();
    Arc::new(up)
}

fn spawn_downlink(
    scenario: Scenario,
    up: Arc<UdpSocket>,
    front: Arc<UdpSocket>,
    client: Arc<tokio::sync::OnceCell<std::net::SocketAddr>>,
    armed: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        let mut batch: Vec<Vec<u8>> = Vec::new();
        let mut count = 0u64;
        loop {
            let wait = if batch.is_empty() { Duration::from_secs(3600) } else { Duration::from_millis(5) };
            let received = tokio::time::timeout(wait, up.recv(&mut buf)).await;
            let Some(&to) = client.get() else { continue };
            let n = match received {
                Ok(Ok(n)) => n,
                Ok(Err(_)) => return,
                Err(_) => {
                    for packet in batch.drain(..).rev() {
                        let _ = front.send_to(&packet, to).await;
                    }
                    continue;
                }
            };
            let packet = &buf[..n];
            if !armed.load(Relaxed) {
                let _ = front.send_to(packet, to).await;
                continue;
            }
            count += 1;
            match scenario {
                Scenario::Duplicate => {
                    for _ in 0..3 {
                        let _ = front.send_to(packet, to).await;
                    }
                }
                Scenario::Reorder => {
                    batch.push(packet.to_vec());
                    if batch.len() == 32 {
                        for packet in batch.drain(..).rev() {
                            let _ = front.send_to(&packet, to).await;
                        }
                    }
                }
                Scenario::Gaps if count % 4 == 0 => {}
                Scenario::TxStormSmall | Scenario::TxStormBulk => {}
                _ => {
                    let _ = front.send_to(packet, to).await;
                }
            }
        }
    });
}

// ---------------------------------------------------------------- receiver

#[derive(Debug)]
struct Report {
    scenario: Scenario,
    endpoint_delta: usize,
    peak_delta: usize,
    meter_peak: usize,
    peer_written: u64,
    local_written: u64,
    rx_datagrams: u64,
    rx_bytes: u64,
    refused: u64,
    closed: Option<String>,
}

struct Child {
    process: std::process::Child,
    dir: PathBuf,
}
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        for name in ["ca.pem", "addr", "done"] {
            let _ = std::fs::remove_file(self.dir.join(name));
        }
        let _ = std::fs::remove_dir(&self.dir);
    }
}

fn spawn_child(scenario: Scenario) -> Child {
    let dir = std::env::temp_dir().join(format!(
        "a2-amp-{}-{:?}-{}",
        std::process::id(),
        scenario,
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    let process = std::process::Command::new(std::env::current_exe().unwrap())
        .args([CHILD, "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env(ENV_SCENARIO, scenario.name())
        .env(ENV_DIR, &dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    Child { process, dir }
}

async fn wait_file(path: &Path, cap: Duration) -> Option<String> {
    let end = Instant::now() + cap;
    while Instant::now() < end {
        if let Ok(text) = std::fs::read_to_string(path) {
            return Some(text);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

/// Shape under measurement: production by default; `MINI_VPN_A2_SHAPE=small`
/// selects the Service-test injection shape (`fixture::native_shape` values).
fn measured_shape() -> TransportShape {
    match std::env::var("MINI_VPN_A2_SHAPE").as_deref() {
        Ok("small") => TransportShape {
            bidi_streams: 8,
            uni_streams: 64,
            stream_window: 256 * 1024,
            connection_window: 1024 * 1024,
            send_window: 1024 * 1024,
            datagram_rx: 64 * 1024,
            datagram_tx: 64 * 1024,
            crypto_buffer: 16 * 1024,
        },
        _ => TransportShape::tuic(MtuPolicy::Default),
    }
}

async fn measure(scenario: Scenario) -> Report {
    let child = spawn_child(scenario);
    let addr: std::net::SocketAddr = wait_file(&child.dir.join("addr"), Duration::from_secs(30))
        .await
        .expect("adversary child did not start")
        .parse()
        .unwrap();
    let shape = measured_shape();
    let cfg = client_quic_config_alpn_shaped(
        child.dir.join("ca.pem").to_str().unwrap(),
        vec![b"h3".to_vec()],
        CcChoice::Bbr,
        MtuPolicy::Default,
        QuicGsoPolicy::Enabled,
        QuicPacingPolicy::QuinnDefault,
        &shape,
    )
    .unwrap();
    // Node protocol tier with ample headroom: the measurement must observe
    // amplification, not a Full refusal.
    let budget = bytes::BackingBudget::with_protocol_reserve(2048 * MIB, 1024 * MIB).unwrap();
    let meter = bytes::BackingMeter::with_protocol_budget(&budget);
    let before_endpoint = live();
    let (endpoint, _) = client_endpoint_with_udp_send_service_metered(
        cfg,
        QuicUdpSendServicePolicy::QuinnDefault,
        QuicPacingPolicy::QuinnDefault,
        meter,
    )
    .unwrap();
    let endpoint_delta = live().saturating_sub(before_endpoint);
    let base = reset_peak();
    let conn = endpoint.connect(addr, "localhost").unwrap().await.unwrap();
    let local_written = Arc::new(AtomicU64::new(0));
    if let Some(chunk) = scenario.tx() {
        let (conn, local_written) = (conn.clone(), local_written.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await; // after arming
            let Ok((mut send, _recv)) = conn.open_bi().await else { return };
            // Production uplink writes `&[u8]` through `poll_write` (a metered
            // copy inside the stack), never caller-owned Bytes.
            let data = vec![3u8; chunk];
            loop {
                match tokio::time::timeout(Duration::from_secs(2), send.write(&data)).await {
                    Ok(Ok(n)) => local_written.fetch_add(n as u64, Relaxed),
                    _ => return, // send window full (blocked) or closed
                };
            }
        });
    }
    let done_path = child.dir.join("done");
    let end = Instant::now() + Duration::from_secs(60);
    let mut meter_peak = 0usize;
    let mut done_at = None;
    let mut peer_written = 0;
    // Stop once the peer is done and neither direction progressed for `quiet`.
    let quiet = Duration::from_secs(2);
    let mut progress = (0u64, 0u64);
    let mut moved = Instant::now();
    while Instant::now() < end {
        meter_peak = meter_peak.max(budget.used());
        if done_at.is_none() {
            if let Ok(text) = std::fs::read_to_string(&done_path) {
                peer_written = text.trim().parse().unwrap_or(0);
                done_at = Some(Instant::now());
            }
        }
        let now = (conn.stats().udp_rx.bytes, local_written.load(Relaxed));
        if now != progress {
            (progress, moved) = (now, Instant::now());
        }
        if (done_at.is_some() && moved.elapsed() > quiet) || conn.close_reason().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let peak_delta = peak().saturating_sub(base);
    let stats = conn.stats();
    let report = Report {
        scenario,
        endpoint_delta,
        peak_delta,
        meter_peak,
        peer_written,
        local_written: local_written.load(Relaxed),
        rx_datagrams: stats.udp_rx.datagrams,
        rx_bytes: stats.udp_rx.bytes,
        refused: stats.udp_rx.admission_refused,
        closed: conn.close_reason().map(|r| r.to_string()),
    };
    conn.close(0u32.into(), b"");
    drop(conn);
    drop(child);
    let _ = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "A2 measurement: process-wide counter, run alone (see module docs)"]
async fn measure_amplification() {
    let shape = measured_shape();
    let unit = node_bound(&shape, &MeasuredFactors::UNMEASURED_UNIT, 1).unwrap();
    let measured = node_bound(&shape, &MeasuredFactors::A2_MEASURED, 1).unwrap();
    let only = std::env::var("MINI_VPN_A2_ONLY").ok();
    for scenario in ALL {
        if only.as_deref().is_some_and(|o| !o.split(',').any(|n| n == scenario.name())) {
            continue;
        }
        let r = measure(scenario).await;
        let buffered = r.peer_written.min(u64::from(shape.connection_window));
        eprintln!(
            "[A2-k] scenario={:?} peak_delta={} meter_peak={} unmetered_over_meter={} endpoint_delta={} peer_written={} local_written={} rx_datagrams={} rx_bytes={} refused={} closed={:?} peak_over_unit_bound={:.4} peak_per_peer_buffered_byte={:.3} unit_bound={unit}",
            r.scenario,
            r.peak_delta,
            r.meter_peak,
            r.peak_delta as i64 - r.meter_peak as i64,
            r.endpoint_delta,
            r.peer_written,
            r.local_written,
            r.rx_datagrams,
            r.rx_bytes,
            r.refused,
            r.closed,
            r.peak_delta as f64 / unit as f64,
            if buffered > 0 { r.peak_delta as f64 / buffered as f64 } else { 0.0 },
        );
        eprintln!(
            "[A2-bound] scenario={:?} honest={} peak={} measured_bound={measured} fits={}",
            r.scenario,
            r.scenario.honest(),
            r.peak_delta,
            r.peak_delta as u64 <= measured
        );
        assert!(r.refused == 0, "ample budget must not refuse: {r:?}");
        if r.scenario.honest() {
            assert!(r.peak_delta as u64 <= measured, "honest peak over measured bound: {r:?}");
        }
    }
}
