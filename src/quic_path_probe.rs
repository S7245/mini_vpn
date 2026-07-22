use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

#[derive(Debug)]
struct UploadProbeClientResult {
    total_bytes: u64,
    elapsed: Duration,
    send_service_stats: Option<QuicUdpSendServiceSnapshot>,
    pacing_stats: ProbePacingStats,
    endpoint_pacing_stats: Option<quinn::EndpointPacingSnapshot>,
    connection_pacing_stats: Option<quinn::EndpointPacingConnectionSnapshot>,
}

#[derive(Debug, Clone, Copy)]
struct ProbePacingStats {
    cwnd: u64,
    current_mtu: u16,
    pacing_uncapped_capacity_bytes: u64,
    pacing_capacity_bytes: u64,
    pacing_tokens_bytes: u64,
    pacing_mtu: u16,
    pacing_cap_active: bool,
    pacing_delay_events: u64,
}

#[derive(Debug)]
struct UploadProbeServerResult {
    total_bytes: u64,
    pattern_errors: u64,
    clean_eof: bool,
}

#[derive(Debug, Clone, Copy)]
struct ProbeQuicStats {
    rtt_us: u64,
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
        rtt_us: stats.path.rtt.as_micros() as u64,
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
        "quinn_probe_stats side={} rtt_us={} cwnd={} sent_packets={} lost_packets={} lost_bytes={} congestion_events={} tx_data_blocked={} tx_stream_data_blocked={} udp_tx_datagrams={} udp_tx_bytes={} udp_rx_datagrams={} udp_rx_bytes={} max_datagram_size={}",
        side,
        stats.rtt_us,
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

async fn close_probe_endpoint(
    connection: &quinn::Connection,
    endpoint: &Endpoint,
) -> Result<(), String> {
    connection.close(0u32.into(), b"probe complete");
    endpoint.close(0u32.into(), b"probe complete");
    tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle())
        .await
        .map_err(|_| "probe endpoint did not become idle after close".to_string())?;
    Ok(())
}

fn probe_server_endpoint(
    bind: SocketAddr,
    cert_path: &str,
    key_path: &str,
) -> Result<Endpoint, String> {
    probe_server_endpoint_with_mtu(bind, cert_path, key_path, MtuPolicy::Safe1200)
}

fn probe_server_endpoint_with_mtu(
    bind: SocketAddr,
    cert_path: &str,
    key_path: &str,
    mtu_policy: MtuPolicy,
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
    server_config.transport_config(quic_transport_config(
        CcChoice::Cubic,
        mtu_policy,
        QuicGsoPolicy::Enabled,
        super::QuicPacingPolicy::QuinnDefault,
    ));

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
    close_probe_endpoint(&connection, &endpoint).await?;
    Ok(ProbeServerResult {
        total_bytes,
        elapsed,
        stats,
    })
}

async fn run_upload_probe_server_once(
    endpoint: Endpoint,
    expected_bytes: u64,
) -> Result<UploadProbeServerResult, String> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| "upload probe server endpoint closed before accept".to_string())?;
    let connection = incoming
        .await
        .map_err(|err| format!("upload probe server handshake: {err}"))?;
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .map_err(|err| format!("upload probe server accept stream: {err}"))?;

    let mut total_bytes = 0u64;
    let mut pattern_errors = 0u64;
    let clean_eof = loop {
        match recv
            .read_chunk(PROBE_READ_MAX_BYTES, true)
            .await
            .map_err(|err| format!("upload probe server read payload: {err}"))?
        {
            Some(chunk) => {
                total_bytes = total_bytes.saturating_add(chunk.bytes.len() as u64);
                pattern_errors = pattern_errors.saturating_add(
                    chunk
                        .bytes
                        .iter()
                        .filter(|byte| **byte != PROBE_PATTERN)
                        .count() as u64,
                );
                if total_bytes > expected_bytes {
                    return Err(format!(
                        "upload probe server received {total_bytes} > expected {expected_bytes}"
                    ));
                }
            }
            None => break true,
        }
    };
    if total_bytes != expected_bytes {
        return Err(format!(
            "upload probe server received {total_bytes} != expected {expected_bytes}"
        ));
    }

    send.write_all(&[PROBE_ACK])
        .await
        .map_err(|err| format!("upload probe server write ACK: {err}"))?;
    send.finish()
        .map_err(|err| format!("upload probe server finish ACK: {err}"))?;
    send.stopped()
        .await
        .map_err(|err| format!("upload probe server wait ACK delivery: {err}"))?;
    close_probe_endpoint(&connection, &endpoint).await?;
    Ok(UploadProbeServerResult {
        total_bytes,
        pattern_errors,
        clean_eof,
    })
}

async fn run_upload_probe_client(
    server_addr: SocketAddr,
    server_name: &str,
    ca_path: &str,
    amount: u64,
    chunk_bytes: usize,
    gso_policy: QuicGsoPolicy,
    send_service_policy: QuicUdpSendServicePolicy,
    pacing_policy: super::QuicPacingPolicy,
) -> Result<UploadProbeClientResult, String> {
    run_upload_probe_client_with_mtu(
        server_addr,
        server_name,
        ca_path,
        amount,
        chunk_bytes,
        MtuPolicy::Safe1200,
        gso_policy,
        send_service_policy,
        pacing_policy,
    )
    .await
}

async fn run_upload_probe_client_with_mtu(
    server_addr: SocketAddr,
    server_name: &str,
    ca_path: &str,
    amount: u64,
    chunk_bytes: usize,
    mtu_policy: MtuPolicy,
    gso_policy: QuicGsoPolicy,
    send_service_policy: QuicUdpSendServicePolicy,
    pacing_policy: super::QuicPacingPolicy,
) -> Result<UploadProbeClientResult, String> {
    super::validate_quic_send_policies(pacing_policy, send_service_policy)?;
    let client_config = client_quic_config_alpn(
        ca_path,
        vec![PROBE_ALPN.to_vec()],
        CcChoice::Cubic,
        mtu_policy,
        gso_policy,
        pacing_policy,
    )?;
    let (endpoint, send_service_stats) =
        client_endpoint_with_udp_send_service(client_config, send_service_policy, pacing_policy)?;
    let connection = endpoint
        .connect(server_addr, server_name)
        .map_err(|err| format!("start upload probe client connect: {err}"))?
        .await
        .map_err(|err| format!("upload probe client handshake: {err}"))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| format!("upload probe client open stream: {err}"))?;

    let payload = vec![PROBE_PATTERN; chunk_bytes];
    let started = Instant::now();
    let mut total_bytes = 0u64;
    while total_bytes < amount {
        let write_bytes = (amount - total_bytes).min(payload.len() as u64) as usize;
        send.write_all(&payload[..write_bytes])
            .await
            .map_err(|err| format!("upload probe client write payload: {err}"))?;
        total_bytes = total_bytes.saturating_add(write_bytes as u64);
    }
    send.finish()
        .map_err(|err| format!("upload probe client finish payload: {err}"))?;

    let mut ack = [0u8; 1];
    recv.read_exact(&mut ack)
        .await
        .map_err(|err| format!("upload probe client read ACK: {err}"))?;
    if ack != [PROBE_ACK] {
        return Err(format!("upload probe client ACK mismatch: {ack:?}"));
    }
    let trailing = recv
        .read_to_end(1)
        .await
        .map_err(|err| format!("upload probe client read ACK EOF: {err}"))?;
    if !trailing.is_empty() {
        return Err(format!(
            "upload probe client received trailing ACK bytes: {}",
            trailing.len()
        ));
    }
    let elapsed = started.elapsed();
    let path = connection.stats().path;
    let pacing_stats = ProbePacingStats {
        cwnd: path.cwnd,
        current_mtu: path.current_mtu,
        pacing_uncapped_capacity_bytes: path.pacing_uncapped_capacity_bytes,
        pacing_capacity_bytes: path.pacing_capacity_bytes,
        pacing_tokens_bytes: path.pacing_tokens_bytes,
        pacing_mtu: path.pacing_mtu,
        pacing_cap_active: path.pacing_cap_active,
        pacing_delay_events: path.pacing_delay_events,
    };
    let connection_pacing_stats = connection.endpoint_pacing_snapshot();
    close_probe_endpoint(&connection, &endpoint).await?;
    let endpoint_pacing_stats = endpoint.endpoint_pacing_snapshot();
    Ok(UploadProbeClientResult {
        total_bytes,
        elapsed,
        send_service_stats: send_service_stats.map(|stats| stats.snapshot()),
        pacing_stats,
        endpoint_pacing_stats,
        connection_pacing_stats,
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
        QuicGsoPolicy::Enabled,
        super::QuicPacingPolicy::QuinnDefault,
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
    close_probe_endpoint(&connection, &endpoint).await?;
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
    let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
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
    let server = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("loopback reverse server must not hang after completion ACK")
        .unwrap()
        .unwrap();

    assert_eq!(client.total_bytes, request.amount);
    assert_eq!(server.total_bytes, request.amount);
    assert_eq!(client.pattern_errors, 0);
    assert!(client.clean_eof);
    assert!(!client.intervals.is_empty());
    let receiver_mbps = client.total_bytes as f64 * 8.0 / client.elapsed.as_secs_f64() / 1e6;
    assert!(receiver_mbps > 170.0, "receiver_mbps={receiver_mbps:.3}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endpoint_window_rebind_preserves_two_live_connections_and_conservation() {
    async fn echo_twice(connection: quinn::Connection) {
        for _ in 0..2 {
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let payload = recv.read_to_end(64).await.unwrap();
            send.write_all(&payload).await.unwrap();
            send.finish().unwrap();
            send.stopped().await.unwrap();
        }
    }

    async fn round_trip(connection: &quinn::Connection, payload: &[u8]) {
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send.write_all(payload).await.unwrap();
        send.finish().unwrap();
        assert_eq!(recv.read_to_end(64).await.unwrap(), payload);
    }

    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_endpoint = probe_server_endpoint(
        "127.0.0.1:0".parse().unwrap(),
        cert_dir.join("server-cert.pem").to_str().unwrap(),
        cert_dir.join("server-key.pem").to_str().unwrap(),
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let connection = server_endpoint.accept().await.unwrap().await.unwrap();
            tasks.push(tokio::spawn(echo_twice(connection)));
        }
        for task in tasks {
            task.await.unwrap();
        }
    });

    let client_config = client_quic_config_alpn(
        cert_dir.join("ca-cert.pem").to_str().unwrap(),
        vec![PROBE_ALPN.to_vec()],
        CcChoice::Cubic,
        MtuPolicy::Safe1200,
        QuicGsoPolicy::Enabled,
        super::QuicPacingPolicy::EndpointWindowV1,
    )
    .unwrap();
    let (endpoint, send_stats) = client_endpoint_with_udp_send_service(
        client_config,
        QuicUdpSendServicePolicy::QuinnDefault,
        super::QuicPacingPolicy::EndpointWindowV1,
    )
    .unwrap();
    let first = endpoint
        .connect(server_addr, "example.com")
        .unwrap()
        .await
        .unwrap();
    let second = endpoint
        .connect(server_addr, "example.com")
        .unwrap()
        .await
        .unwrap();

    tokio::join!(
        round_trip(&first, b"before-1"),
        round_trip(&second, b"before-2")
    );
    let before_addr = endpoint.local_addr().unwrap();
    let before_pacing = endpoint.endpoint_pacing_snapshot().unwrap();

    let rebound = rebind_client_endpoint_udp_socket(
        &endpoint,
        QuicUdpSendServicePolicy::QuinnDefault,
        send_stats.as_ref(),
    )
    .unwrap();
    assert_eq!(rebound.old_local_addr, before_addr);
    assert_ne!(rebound.new_local_addr.port(), before_addr.port());
    assert_eq!(rebound.rebind_generation, 1);
    assert_eq!(endpoint.stats().current_socket_rx_rebind_generation, 0);
    assert_eq!(first.current_socket_rx_rebind_generation(), 0);
    assert_eq!(second.current_socket_rx_rebind_generation(), 0);
    assert_eq!(endpoint.local_addr().unwrap(), rebound.new_local_addr);

    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            round_trip(&first, b"after-1"),
            round_trip(&second, b"after-2")
        );
    })
    .await
    .expect("both established connections must survive endpoint socket rebind");
    assert_eq!(endpoint.stats().current_socket_rx_rebind_generation, 1);
    assert_eq!(first.current_socket_rx_rebind_generation(), 1);
    assert_eq!(second.current_socket_rx_rebind_generation(), 1);

    let after_pacing = endpoint.endpoint_pacing_snapshot().unwrap();
    assert_eq!(
        after_pacing.rate_bytes_per_second,
        before_pacing.rate_bytes_per_second
    );
    assert_eq!(after_pacing.burst_bytes, before_pacing.burst_bytes);
    assert!(
        after_pacing.available_tokens
            + after_pacing.live_reservation_bytes
            + after_pacing.outstanding_bytes
            <= after_pacing.burst_bytes
    );
    server_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disabled_gso_real_loopback_upload_delivers_fixed_bytes_and_clean_eof() {
    let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_endpoint = probe_server_endpoint(
        "127.0.0.1:0".parse().unwrap(),
        cert_dir.join("server-cert.pem").to_str().unwrap(),
        cert_dir.join("server-key.pem").to_str().unwrap(),
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();
    let amount = 32 * 1024 * 1024;
    let server_task = tokio::spawn(run_upload_probe_server_once(server_endpoint, amount));

    let client = tokio::time::timeout(
        Duration::from_secs(10),
        run_upload_probe_client(
            server_addr,
            "example.com",
            cert_dir.join("ca-cert.pem").to_str().unwrap(),
            amount,
            64 * 1024,
            QuicGsoPolicy::Disabled,
            QuicUdpSendServicePolicy::QuinnDefault,
            super::QuicPacingPolicy::QuinnDefault,
        ),
    )
    .await
    .expect("disabled-GSO loopback upload must not time out")
    .unwrap();
    let server = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("disabled-GSO upload server must not hang after ACK EOF")
        .unwrap()
        .unwrap();

    assert_eq!(client.total_bytes, amount);
    assert_eq!(server.total_bytes, amount);
    assert_eq!(server.pattern_errors, 0);
    assert!(server.clean_eof);
    let sender_mbps = client.total_bytes as f64 * 8.0 / client.elapsed.as_secs_f64() / 1e6;
    assert!(sender_mbps > 170.0, "sender_mbps={sender_mbps:.3}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pacer_cap64_real_loopback_upload_delivers_fixed_bytes_and_clean_eof() {
    let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_endpoint = probe_server_endpoint(
        "127.0.0.1:0".parse().unwrap(),
        cert_dir.join("server-cert.pem").to_str().unwrap(),
        cert_dir.join("server-key.pem").to_str().unwrap(),
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();
    let amount = 32 * 1024 * 1024;
    let server_task = tokio::spawn(run_upload_probe_server_once(server_endpoint, amount));

    let client = tokio::time::timeout(
        Duration::from_secs(10),
        run_upload_probe_client(
            server_addr,
            "example.com",
            cert_dir.join("ca-cert.pem").to_str().unwrap(),
            amount,
            64 * 1024,
            QuicGsoPolicy::Enabled,
            QuicUdpSendServicePolicy::QuinnDefault,
            super::QuicPacingPolicy::PacerCap64,
        ),
    )
    .await
    .expect("pacer-cap64 loopback upload must not time out")
    .unwrap();
    let server = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("pacer-cap64 upload server must not hang after ACK EOF")
        .unwrap()
        .unwrap();

    assert_eq!(client.total_bytes, amount);
    assert_eq!(server.total_bytes, amount);
    assert_eq!(server.pattern_errors, 0);
    assert!(server.clean_eof);
    assert!(client.send_service_stats.is_none());

    let pacing = client.pacing_stats;
    let expected_capacity = 64 * u64::from(pacing.pacing_mtu);
    assert!(pacing.cwnd <= u64::from(u32::MAX), "{pacing:?}");
    assert!(pacing.pacing_cap_active, "{pacing:?}");
    assert_eq!(
        pacing.pacing_capacity_bytes, expected_capacity,
        "{pacing:?}"
    );
    assert!(
        pacing.pacing_tokens_bytes <= pacing.pacing_capacity_bytes,
        "{pacing:?}"
    );
    assert!(
        pacing.pacing_uncapped_capacity_bytes > expected_capacity,
        "{pacing:?}"
    );
    assert!(pacing.pacing_delay_events > 0, "{pacing:?}");
    assert_eq!(pacing.pacing_mtu, pacing.current_mtu, "{pacing:?}");

    let sender_mbps = client.total_bytes as f64 * 8.0 / client.elapsed.as_secs_f64() / 1e6;
    eprintln!("pacer_cap64_probe sender_mbps={sender_mbps:.3} pacing={pacing:?}");
    assert!(sender_mbps > 170.0, "sender_mbps={sender_mbps:.3}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endpoint_window_v1_real_loopback_upload_exceeds_capacity_gate_without_leaks() {
    let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_endpoint = probe_server_endpoint_with_mtu(
        "127.0.0.1:0".parse().unwrap(),
        cert_dir.join("server-cert.pem").to_str().unwrap(),
        cert_dir.join("server-key.pem").to_str().unwrap(),
        MtuPolicy::Default,
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();
    let amount = 32 * 1024 * 1024;
    let server_task = tokio::spawn(run_upload_probe_server_once(server_endpoint, amount));

    let client = tokio::time::timeout(
        Duration::from_secs(10),
        run_upload_probe_client_with_mtu(
            server_addr,
            "example.com",
            cert_dir.join("ca-cert.pem").to_str().unwrap(),
            amount,
            64 * 1024,
            MtuPolicy::Default,
            QuicGsoPolicy::Enabled,
            QuicUdpSendServicePolicy::QuinnDefault,
            super::QuicPacingPolicy::EndpointWindowV1,
        ),
    )
    .await
    .expect("endpoint-window-v1 loopback upload must not time out")
    .unwrap();
    let server = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("endpoint-window-v1 upload server must not hang after ACK EOF")
        .unwrap()
        .unwrap();

    assert_eq!(client.total_bytes, amount);
    assert_eq!(server.total_bytes, amount);
    assert_eq!(server.pattern_errors, 0);
    assert!(server.clean_eof);
    assert!(client.send_service_stats.is_none());
    assert!(
        !client.pacing_stats.pacing_cap_active,
        "{:?}",
        client.pacing_stats
    );
    assert_eq!(
        client.pacing_stats.pacing_capacity_bytes,
        client.pacing_stats.pacing_uncapped_capacity_bytes,
        "the rejected per-connection cap must remain inactive"
    );

    let connection = client
        .connection_pacing_stats
        .expect("configured connection must expose endpoint service attribution");
    assert!(connection.attached);
    assert!(connection.granted_bulk_bytes >= amount);
    assert!(connection.granted_control_bytes > 0);
    assert_eq!(connection.live_reservation_bytes, 0);
    assert_eq!(connection.outstanding_bytes, 0);

    let endpoint = client
        .endpoint_pacing_stats
        .expect("configured endpoint must retain its service snapshot after idle");
    assert_eq!(endpoint.rate_bytes_per_second, 30_720_000);
    assert_eq!(endpoint.burst_bytes, 61_440);
    assert_eq!(endpoint.control_reserve_bytes, 10_240);
    assert_eq!(endpoint.connection_quantum_bytes, 20_480);
    assert!(endpoint.endpoint_delay_events > 0, "{endpoint:?}");
    assert!(endpoint.max_endpoint_delay_nanos > 0, "{endpoint:?}");
    assert!(endpoint.outstanding_bytes_high_water > 0, "{endpoint:?}");
    assert_eq!(endpoint.live_reservation_bytes, 0, "{endpoint:?}");
    assert_eq!(endpoint.outstanding_bytes, 0, "{endpoint:?}");
    assert_eq!(endpoint.connection_records, 0, "{endpoint:?}");
    assert!(endpoint.detaches > 0, "{endpoint:?}");
    assert_eq!(endpoint.stateless_responses_sent, 0, "{endpoint:?}");
    assert_eq!(endpoint.stateless_responses_dropped, 0, "{endpoint:?}");
    assert_eq!(
        endpoint.granted_bytes,
        endpoint
            .refunded_bytes
            .saturating_add(endpoint.sent_bytes)
            .saturating_add(endpoint.abandoned_bytes),
        "every reservation must have exactly one terminal accounting path: {endpoint:?}"
    );
    assert!(
        endpoint.available_tokens + endpoint.live_reservation_bytes + endpoint.outstanding_bytes
            <= endpoint.burst_bytes,
        "endpoint conservation failed: {endpoint:?}"
    );

    let sender_mbps = client.total_bytes as f64 * 8.0 / client.elapsed.as_secs_f64() / 1e6;
    eprintln!(
        "endpoint_window_v1_probe sender_mbps={sender_mbps:.3} endpoint={endpoint:?} connection={connection:?}"
    );
    assert!(sender_mbps > 170.0, "sender_mbps={sender_mbps:.3}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "known-negative fixed 48-datagram/2ms replay; run explicitly for measurement only"]
async fn known_negative_bounded_send_service_real_loopback_upload_measurement() {
    let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_endpoint = probe_server_endpoint(
        "127.0.0.1:0".parse().unwrap(),
        cert_dir.join("server-cert.pem").to_str().unwrap(),
        cert_dir.join("server-key.pem").to_str().unwrap(),
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();
    let amount = 32 * 1024 * 1024;
    let server_task = tokio::spawn(run_upload_probe_server_once(server_endpoint, amount));

    let client = tokio::time::timeout(
        Duration::from_secs(10),
        run_upload_probe_client(
            server_addr,
            "example.com",
            cert_dir.join("ca-cert.pem").to_str().unwrap(),
            amount,
            64 * 1024,
            QuicGsoPolicy::Enabled,
            QuicUdpSendServicePolicy::Bounded,
            super::QuicPacingPolicy::QuinnDefault,
        ),
    )
    .await
    .expect("bounded-send-service loopback upload must not time out")
    .unwrap();
    let server = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("bounded-send-service upload server must not hang after ACK EOF")
        .unwrap()
        .unwrap();

    assert_eq!(client.total_bytes, amount);
    assert_eq!(server.total_bytes, amount);
    assert_eq!(server.pattern_errors, 0);
    assert!(server.clean_eof);
    let service = client
        .send_service_stats
        .expect("bounded endpoint exposes send-service stats");
    assert!(service.accepted_bytes >= amount);
    assert!(service.accepted_datagrams > 0);
    assert!(service.cooldown_rearms > 0);
    let sender_mbps = client.total_bytes as f64 * 8.0 / client.elapsed.as_secs_f64() / 1e6;
    let mean_payload_bytes = service.accepted_bytes as f64 / service.accepted_datagrams as f64;
    let mean_rearm_period_us =
        service.service_elapsed_ns as f64 / service.cooldown_rearms as f64 / 1_000.0;
    let mean_rearm_lateness_us =
        service.total_rearm_lateness_ns as f64 / service.cooldown_rearms as f64 / 1_000.0;
    eprintln!(
        "bounded_send_service_probe sender_mbps={sender_mbps:.3} mean_payload_bytes={mean_payload_bytes:.3} mean_rearm_period_us={mean_rearm_period_us:.3} mean_rearm_lateness_us={mean_rearm_lateness_us:.3} service={service:?}"
    );
    assert!(sender_mbps > 170.0, "sender_mbps={sender_mbps:.3}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run explicitly on the Knife14 VPS path"]
async fn cross_host_safe1200_reverse_stream_probe() {
    run_cross_host_probe_from_env().await.unwrap();
}
