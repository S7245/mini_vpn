use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Endpoint, ServerConfig};

const PROBE_MAGIC: [u8; 8] = *b"K14QP001";
const PROBE_VERSION: u32 = 1;
const PROBE_FRAME_BYTES: usize = 32;
const PROBE_CHUNK_MAX_BYTES: u32 = 256 * 1024;
const PROBE_FIXED_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const PROBE_DURATION_MAX_MILLIS: u64 = 30_000;
const PROBE_READ_MAX_BYTES: usize = 128 * 1024;
const PROBE_PATTERN: u8 = 0xa5;
const PROBE_ACK: u8 = 0xac;
const PROBE_ALPN: &[u8] = b"mvpn-knife14-quinn-path-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeMode {
    FixedBytes = 1,
    DurationMillis = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProbeRequest {
    mode: ProbeMode,
    amount: u64,
    chunk_bytes: u32,
}

impl ProbeRequest {
    fn encode(self) -> [u8; PROBE_FRAME_BYTES] {
        let mut frame = [0u8; PROBE_FRAME_BYTES];
        frame[0..8].copy_from_slice(&PROBE_MAGIC);
        frame[8..12].copy_from_slice(&PROBE_VERSION.to_be_bytes());
        frame[12..16].copy_from_slice(&(self.mode as u32).to_be_bytes());
        frame[16..24].copy_from_slice(&self.amount.to_be_bytes());
        frame[24..28].copy_from_slice(&self.chunk_bytes.to_be_bytes());
        frame
    }

    fn decode(frame: &[u8]) -> Result<Self, String> {
        if frame.len() != PROBE_FRAME_BYTES {
            return Err(format!(
                "probe request length {} != {PROBE_FRAME_BYTES}",
                frame.len()
            ));
        }
        if frame[0..8] != PROBE_MAGIC {
            return Err("probe request magic mismatch".to_string());
        }
        let version = u32::from_be_bytes(frame[8..12].try_into().unwrap());
        if version != PROBE_VERSION {
            return Err(format!("unsupported probe version {version}"));
        }
        let mode = match u32::from_be_bytes(frame[12..16].try_into().unwrap()) {
            1 => ProbeMode::FixedBytes,
            2 => ProbeMode::DurationMillis,
            raw => return Err(format!("unsupported probe mode {raw}")),
        };
        let amount = u64::from_be_bytes(frame[16..24].try_into().unwrap());
        if amount == 0 {
            return Err("probe amount must be positive".to_string());
        }
        match mode {
            ProbeMode::FixedBytes if amount > PROBE_FIXED_MAX_BYTES => {
                return Err(format!("probe fixed bytes exceed limit: {amount}"));
            }
            ProbeMode::DurationMillis if amount > PROBE_DURATION_MAX_MILLIS => {
                return Err(format!("probe duration exceeds limit: {amount}ms"));
            }
            _ => {}
        }
        let chunk_bytes = u32::from_be_bytes(frame[24..28].try_into().unwrap());
        if !(1..=PROBE_CHUNK_MAX_BYTES).contains(&chunk_bytes) {
            return Err(format!("probe chunk bytes out of range: {chunk_bytes}"));
        }
        if frame[28..32].iter().any(|byte| *byte != 0) {
            return Err("probe reserved bytes must be zero".to_string());
        }
        Ok(Self {
            mode,
            amount,
            chunk_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProbeInterval {
    end_millis: u64,
    duration_millis: u64,
    bytes: u64,
}

impl ProbeInterval {
    fn mbps(self) -> f64 {
        self.bytes as f64 * 8.0 / (self.duration_millis.max(1) as f64 * 1000.0)
    }
}

#[derive(Debug)]
struct ProbeClientResult {
    total_bytes: u64,
    pattern_errors: u64,
    clean_eof: bool,
    elapsed: Duration,
    intervals: Vec<ProbeInterval>,
    stats: ProbeQuicStats,
}

#[derive(Debug)]
struct ProbeServerResult {
    total_bytes: u64,
    elapsed: Duration,
    stats: ProbeQuicStats,
}

#[derive(Debug, Clone, Copy)]
struct ProbeQuicStats {
    rtt_ms: u64,
    cwnd: u64,
    sent_packets: u64,
    lost_packets: u64,
    lost_bytes: u64,
    congestion_events: u64,
    tx_data_blocked: u64,
    tx_stream_data_blocked: u64,
    udp_tx_datagrams: u64,
    udp_tx_bytes: u64,
    udp_rx_datagrams: u64,
    udp_rx_bytes: u64,
    max_datagram_size: u64,
}

fn probe_quic_stats(connection: &quinn::Connection) -> ProbeQuicStats {
    let stats = connection.stats();
    ProbeQuicStats {
        rtt_ms: stats.path.rtt.as_millis() as u64,
        cwnd: stats.path.cwnd,
        sent_packets: stats.path.sent_packets,
        lost_packets: stats.path.lost_packets,
        lost_bytes: stats.path.lost_bytes,
        congestion_events: stats.path.congestion_events,
        tx_data_blocked: stats.frame_tx.data_blocked,
        tx_stream_data_blocked: stats.frame_tx.stream_data_blocked,
        udp_tx_datagrams: stats.udp_tx.datagrams,
        udp_tx_bytes: stats.udp_tx.bytes,
        udp_rx_datagrams: stats.udp_rx.datagrams,
        udp_rx_bytes: stats.udp_rx.bytes,
        max_datagram_size: connection.max_datagram_size().unwrap_or(0) as u64,
    }
}

fn print_probe_stats(side: &str, stats: ProbeQuicStats) {
    println!(
        "quinn_probe_stats side={} rtt_ms={} cwnd={} sent_packets={} lost_packets={} lost_bytes={} congestion_events={} tx_data_blocked={} tx_stream_data_blocked={} udp_tx_datagrams={} udp_tx_bytes={} udp_rx_datagrams={} udp_rx_bytes={} max_datagram_size={}",
        side,
        stats.rtt_ms,
        stats.cwnd,
        stats.sent_packets,
        stats.lost_packets,
        stats.lost_bytes,
        stats.congestion_events,
        stats.tx_data_blocked,
        stats.tx_stream_data_blocked,
        stats.udp_tx_datagrams,
        stats.udp_tx_bytes,
        stats.udp_rx_datagrams,
        stats.udp_rx_bytes,
        stats.max_datagram_size,
    );
}

fn probe_server_endpoint(
    bind: SocketAddr,
    cert_path: &str,
    key_path: &str,
) -> Result<Endpoint, String> {
    let certs = load_certs(cert_path)?;
    let key_file =
        std::fs::File::open(key_path).map_err(|err| format!("open probe key {key_path}: {err}"))?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|err| format!("read probe key {key_path}: {err}"))?
        .ok_or_else(|| format!("no private key in {key_path}"))?;
    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| format!("build probe server TLS: {err}"))?;
    crypto.alpn_protocols = vec![PROBE_ALPN.to_vec()];
    let quic_crypto = QuicServerConfig::try_from(crypto)
        .map_err(|err| format!("build probe QUIC server crypto: {err}"))?;
    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport_config(quic_transport_config(CcChoice::Cubic, MtuPolicy::Safe1200));

    let socket = std::net::UdpSocket::bind(bind)
        .map_err(|err| format!("bind probe server {bind}: {err}"))?;
    configure_quic_udp_socket_buffers(&socket, QUIC_UDP_SOCKET_BUFFER_BYTES)?;
    let runtime = quinn::default_runtime()
        .ok_or_else(|| "no async runtime for probe server endpoint".to_string())?;
    let mut endpoint_config = quinn::EndpointConfig::default();
    endpoint_config
        .max_udp_payload_size(QUIC_MAX_UDP_PAYLOAD_SIZE)
        .map_err(|err| format!("probe max_udp_payload_size: {err:?}"))?;
    Endpoint::new(endpoint_config, Some(server_config), socket, runtime)
        .map_err(|err| format!("create probe server endpoint: {err}"))
}

async fn run_probe_server_once(endpoint: Endpoint) -> Result<ProbeServerResult, String> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| "probe server endpoint closed before accept".to_string())?;
    let connection = incoming
        .await
        .map_err(|err| format!("probe server handshake: {err}"))?;
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .map_err(|err| format!("probe server accept stream: {err}"))?;
    let mut frame = [0u8; PROBE_FRAME_BYTES];
    recv.read_exact(&mut frame)
        .await
        .map_err(|err| format!("probe server read request: {err}"))?;
    let request = ProbeRequest::decode(&frame)?;
    let payload = vec![PROBE_PATTERN; request.chunk_bytes as usize];
    let started = Instant::now();
    let mut total_bytes = 0u64;
    loop {
        let write_bytes = match request.mode {
            ProbeMode::FixedBytes => {
                let remaining = request.amount.saturating_sub(total_bytes);
                if remaining == 0 {
                    break;
                }
                remaining.min(payload.len() as u64) as usize
            }
            ProbeMode::DurationMillis => {
                if started.elapsed() >= Duration::from_millis(request.amount) {
                    break;
                }
                payload.len()
            }
        };
        send.write_all(&payload[..write_bytes])
            .await
            .map_err(|err| format!("probe server write payload: {err}"))?;
        total_bytes = total_bytes.saturating_add(write_bytes as u64);
    }
    send.finish()
        .map_err(|err| format!("probe server finish payload: {err}"))?;
    let mut ack = [0u8; 1];
    recv.read_exact(&mut ack)
        .await
        .map_err(|err| format!("probe server read completion ACK: {err}"))?;
    if ack != [PROBE_ACK] {
        return Err(format!("probe server completion ACK mismatch: {ack:?}"));
    }
    let elapsed = started.elapsed();
    let stats = probe_quic_stats(&connection);
    connection.close(0u32.into(), b"probe complete");
    endpoint.wait_idle().await;
    Ok(ProbeServerResult {
        total_bytes,
        elapsed,
        stats,
    })
}

async fn sample_probe_intervals(
    total_bytes: Arc<AtomicU64>,
    mut done: tokio::sync::watch::Receiver<bool>,
    started: Instant,
    report: bool,
) -> Vec<ProbeInterval> {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut previous = 0u64;
    let mut previous_end_millis = 0u64;
    let mut intervals = Vec::new();
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let current = total_bytes.load(Ordering::Relaxed);
                let end_millis = started.elapsed().as_millis() as u64;
                let sample = ProbeInterval {
                    end_millis,
                    duration_millis: end_millis.saturating_sub(previous_end_millis).max(1),
                    bytes: current.saturating_sub(previous),
                };
                if report {
                    println!(
                        "quinn_probe_interval end_ms={} bytes={} mbps={:.3}",
                        sample.end_millis,
                        sample.bytes,
                        sample.mbps(),
                    );
                }
                intervals.push(sample);
                previous = current;
                previous_end_millis = end_millis;
            }
            changed = done.changed() => {
                if changed.is_err() || *done.borrow() {
                    let current = total_bytes.load(Ordering::Relaxed);
                    let delta = current.saturating_sub(previous);
                    if delta > 0 || intervals.is_empty() {
                        let end_millis = started.elapsed().as_millis() as u64;
                        intervals.push(ProbeInterval {
                            end_millis,
                            duration_millis: end_millis
                                .saturating_sub(previous_end_millis)
                                .max(1),
                            bytes: delta,
                        });
                    }
                    return intervals;
                }
            }
        }
    }
}

async fn run_probe_client(
    server_addr: SocketAddr,
    server_name: &str,
    ca_path: &str,
    request: ProbeRequest,
    report_intervals: bool,
) -> Result<ProbeClientResult, String> {
    let client_config = client_quic_config_alpn(
        ca_path,
        vec![PROBE_ALPN.to_vec()],
        CcChoice::Cubic,
        MtuPolicy::Safe1200,
    )?;
    let endpoint = client_endpoint(client_config)?;
    let connection = endpoint
        .connect(server_addr, server_name)
        .map_err(|err| format!("start probe client connect: {err}"))?
        .await
        .map_err(|err| format!("probe client handshake: {err}"))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| format!("probe client open stream: {err}"))?;
    send.write_all(&request.encode())
        .await
        .map_err(|err| format!("probe client write request: {err}"))?;

    let total_bytes = Arc::new(AtomicU64::new(0));
    let (done_tx, done_rx) = tokio::sync::watch::channel(false);
    let started = Instant::now();
    let sampler = tokio::spawn(sample_probe_intervals(
        total_bytes.clone(),
        done_rx,
        started,
        report_intervals,
    ));
    let mut pattern_errors = 0u64;
    let clean_eof = loop {
        match recv
            .read_chunk(PROBE_READ_MAX_BYTES, true)
            .await
            .map_err(|err| format!("probe client read payload: {err}"))?
        {
            Some(chunk) => {
                pattern_errors = pattern_errors.saturating_add(
                    chunk
                        .bytes
                        .iter()
                        .filter(|byte| **byte != PROBE_PATTERN)
                        .count() as u64,
                );
                total_bytes.fetch_add(chunk.bytes.len() as u64, Ordering::Relaxed);
            }
            None => break true,
        }
    };
    let elapsed = started.elapsed();
    send.write_all(&[PROBE_ACK])
        .await
        .map_err(|err| format!("probe client write completion ACK: {err}"))?;
    send.finish()
        .map_err(|err| format!("probe client finish request stream: {err}"))?;
    let _ = done_tx.send(true);
    let intervals = sampler
        .await
        .map_err(|err| format!("probe interval sampler join: {err}"))?;
    let total_bytes = total_bytes.load(Ordering::Relaxed);
    let stats = probe_quic_stats(&connection);
    connection.close(0u32.into(), b"probe complete");
    endpoint.wait_idle().await;
    Ok(ProbeClientResult {
        total_bytes,
        pattern_errors,
        clean_eof,
        elapsed,
        intervals,
        stats,
    })
}

fn required_env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

fn parse_env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .map_err(|err| format!("invalid {name}={raw:?}: {err}")),
        Err(_) => Ok(default),
    }
}

async fn run_cross_host_probe_from_env() -> Result<(), String> {
    match required_env("MINI_VPN_KNIFE14_QUINN_PROBE_ROLE")?.as_str() {
        "server" => {
            let bind = std::env::var("MINI_VPN_KNIFE14_QUINN_PROBE_BIND")
                .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
                .parse::<SocketAddr>()
                .map_err(|err| format!("invalid probe bind: {err}"))?;
            let cert_path = required_env("MINI_VPN_KNIFE14_QUINN_PROBE_CERT")?;
            let key_path = required_env("MINI_VPN_KNIFE14_QUINN_PROBE_KEY")?;
            let endpoint = probe_server_endpoint(bind, &cert_path, &key_path)?;
            println!(
                "quinn_probe_server_ready bind={} profile=cubic_safe1200 alpn={}",
                endpoint
                    .local_addr()
                    .map_err(|err| format!("probe server local addr: {err}"))?,
                String::from_utf8_lossy(PROBE_ALPN),
            );
            let result = run_probe_server_once(endpoint).await?;
            let sender_mbps = result.total_bytes as f64 * 8.0 / result.elapsed.as_secs_f64() / 1e6;
            println!(
                "quinn_probe_server_result bytes={} elapsed_ms={} sender_mbps={:.3}",
                result.total_bytes,
                result.elapsed.as_millis(),
                sender_mbps,
            );
            print_probe_stats("server", result.stats);
            Ok(())
        }
        "client" => {
            let server_addr = required_env("MINI_VPN_KNIFE14_QUINN_PROBE_SERVER")?
                .parse::<SocketAddr>()
                .map_err(|err| format!("invalid probe server address: {err}"))?;
            let server_name = required_env("MINI_VPN_KNIFE14_QUINN_PROBE_SNI")?;
            let ca_path = required_env("MINI_VPN_KNIFE14_QUINN_PROBE_CA")?;
            let duration_secs = parse_env_u64("MINI_VPN_KNIFE14_QUINN_PROBE_DURATION_SECS", 20)?;
            let floor_mbps = parse_env_u64("MINI_VPN_KNIFE14_QUINN_PROBE_FLOOR_MBPS", 150)?;
            let chunk_bytes = parse_env_u64("MINI_VPN_KNIFE14_QUINN_PROBE_CHUNK_BYTES", 64 * 1024)?;
            if !(1..=PROBE_CHUNK_MAX_BYTES as u64).contains(&chunk_bytes) {
                return Err(format!("probe chunk bytes out of range: {chunk_bytes}"));
            }
            let request = ProbeRequest {
                mode: ProbeMode::DurationMillis,
                amount: duration_secs.saturating_mul(1000),
                chunk_bytes: chunk_bytes as u32,
            };
            println!(
                "quinn_probe_client_start server={} profile=cubic_safe1200 duration_secs={} chunk_bytes={} floor_mbps={} alpn={}",
                server_addr,
                duration_secs,
                chunk_bytes,
                floor_mbps,
                String::from_utf8_lossy(PROBE_ALPN),
            );
            let result = tokio::time::timeout(
                Duration::from_secs(duration_secs.saturating_add(30)),
                run_probe_client(server_addr, &server_name, &ca_path, request, true),
            )
            .await
            .map_err(|_| "cross-host probe client timed out".to_string())??;
            let receiver_mbps =
                result.total_bytes as f64 * 8.0 / result.elapsed.as_secs_f64() / 1e6;
            let zero_intervals = result
                .intervals
                .iter()
                .filter(|sample| sample.bytes == 0)
                .count();
            let min_interval_mbps = result
                .intervals
                .iter()
                .filter(|sample| sample.bytes > 0)
                .map(|sample| sample.mbps())
                .reduce(f64::min)
                .unwrap_or(0.0);
            println!(
                "quinn_probe_client_result bytes={} elapsed_ms={} receiver_mbps={:.3} intervals={} zero_intervals={} min_interval_mbps={:.3} pattern_errors={} clean_eof={}",
                result.total_bytes,
                result.elapsed.as_millis(),
                receiver_mbps,
                result.intervals.len(),
                zero_intervals,
                min_interval_mbps,
                result.pattern_errors,
                result.clean_eof,
            );
            print_probe_stats("client", result.stats);
            if receiver_mbps <= floor_mbps as f64 {
                return Err(format!(
                    "quinn probe receiver {receiver_mbps:.3} <= floor {floor_mbps}"
                ));
            }
            if zero_intervals != 0 {
                return Err(format!("quinn probe zero intervals: {zero_intervals}"));
            }
            if result.pattern_errors != 0 || !result.clean_eof {
                return Err(format!(
                    "quinn probe integrity failed: pattern_errors={} clean_eof={}",
                    result.pattern_errors, result.clean_eof
                ));
            }
            println!("quinn_probe_result=PASS");
            Ok(())
        }
        role => Err(format!("unsupported Quinn probe role {role:?}")),
    }
}

#[test]
fn request_round_trip_and_rejects_invalid_frames() {
    let request = ProbeRequest {
        mode: ProbeMode::FixedBytes,
        amount: 32 * 1024 * 1024,
        chunk_bytes: 64 * 1024,
    };
    let encoded = request.encode();
    assert_eq!(ProbeRequest::decode(&encoded).unwrap(), request);

    let mut bad_magic = encoded;
    bad_magic[0] ^= 0xff;
    assert!(ProbeRequest::decode(&bad_magic).is_err());

    let mut bad_chunk = encoded;
    bad_chunk[24..28].copy_from_slice(&0u32.to_be_bytes());
    assert!(ProbeRequest::decode(&bad_chunk).is_err());

    let mut bad_reserved = encoded;
    bad_reserved[31] = 1;
    assert!(ProbeRequest::decode(&bad_reserved).is_err());

    let too_long = ProbeRequest {
        mode: ProbeMode::DurationMillis,
        amount: 30_001,
        chunk_bytes: 64 * 1024,
    }
    .encode();
    assert!(ProbeRequest::decode(&too_long).is_err());

    let too_large = ProbeRequest {
        mode: ProbeMode::FixedBytes,
        amount: 2 * 1024 * 1024 * 1024 + 1,
        chunk_bytes: 64 * 1024,
    }
    .encode();
    assert!(ProbeRequest::decode(&too_large).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_loopback_reverse_stream_delivers_fixed_bytes_and_clean_eof() {
    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_endpoint = probe_server_endpoint(
        "127.0.0.1:0".parse().unwrap(),
        cert_dir.join("server-cert.pem").to_str().unwrap(),
        cert_dir.join("server-key.pem").to_str().unwrap(),
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();
    let server_task = tokio::spawn(run_probe_server_once(server_endpoint));

    let request = ProbeRequest {
        mode: ProbeMode::FixedBytes,
        amount: 32 * 1024 * 1024,
        chunk_bytes: 64 * 1024,
    };
    let client = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_probe_client(
            server_addr,
            "example.com",
            cert_dir.join("ca-cert.pem").to_str().unwrap(),
            request,
            false,
        ),
    )
    .await
    .expect("loopback probe must not time out")
    .unwrap();
    let server = server_task.await.unwrap().unwrap();

    assert_eq!(client.total_bytes, request.amount);
    assert_eq!(server.total_bytes, request.amount);
    assert_eq!(client.pattern_errors, 0);
    assert!(client.clean_eof);
    assert!(!client.intervals.is_empty());
    let receiver_mbps = client.total_bytes as f64 * 8.0 / client.elapsed.as_secs_f64() / 1e6;
    assert!(receiver_mbps > 170.0, "receiver_mbps={receiver_mbps:.3}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run explicitly on the Knife14 VPS path"]
async fn cross_host_safe1200_reverse_stream_probe() {
    run_cross_host_probe_from_env().await.unwrap();
}
