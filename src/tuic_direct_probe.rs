use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use super::*;

#[derive(Debug, Default, PartialEq, Eq)]
struct BridgeResult {
    accepted_connections: usize,
    completed_connections: usize,
    terminal_reset_connections: usize,
    failed_connections: usize,
    local_to_remote_bytes: u64,
    remote_to_local_bytes: u64,
    first_terminal_reset: Option<String>,
    first_failure: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BridgeFailureKind {
    TerminalReset,
    Unexpected,
}

#[derive(Debug)]
struct BridgeFailure {
    kind: BridgeFailureKind,
    message: String,
}

impl BridgeFailure {
    fn unexpected(message: String) -> Self {
        Self {
            kind: BridgeFailureKind::Unexpected,
            message,
        }
    }

    fn relay_io(target: &TargetAddr, err: std::io::Error) -> Self {
        let kind = match err.kind() {
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe => {
                BridgeFailureKind::TerminalReset
            }
            _ => BridgeFailureKind::Unexpected,
        };
        Self {
            kind,
            message: format!("direct TUIC probe relay {target:?}: {err}"),
        }
    }
}

#[derive(Debug, PartialEq)]
struct IperfProbeResult {
    receiver_mbps: f64,
    intervals: usize,
    zero_intervals: usize,
    min_interval_mbps: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimedCapacityTerminal {
    Clean,
    OneReset,
}

impl TimedCapacityTerminal {
    fn as_str(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::OneReset => "one_reset",
        }
    }
}

#[derive(Debug, PartialEq)]
struct TimedCapacityProbeResult {
    receiver_mbps: f64,
    intervals: usize,
    zero_intervals: usize,
    min_interval_mbps: f64,
    terminal: TimedCapacityTerminal,
}

fn validate_reverse_iperf3_json(
    raw: &str,
    expected_intervals: usize,
    floor_mbps: f64,
) -> Result<IperfProbeResult, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|err| format!("direct TUIC probe parse iperf3 JSON: {err}"))?;
    let intervals = value
        .get("intervals")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "direct TUIC probe iperf3 JSON has no intervals".to_string())?;
    if intervals.len() != expected_intervals {
        return Err(format!(
            "direct TUIC probe expected {expected_intervals} intervals, got {}",
            intervals.len()
        ));
    }
    let interval_mbps = intervals
        .iter()
        .map(|interval| {
            interval
                .get("sum")
                .and_then(|sum| sum.get("bits_per_second"))
                .and_then(serde_json::Value::as_f64)
                .map(|bits_per_second| bits_per_second / 1_000_000.0)
                .ok_or_else(|| "direct TUIC probe iperf3 interval has no rate".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let zero_intervals = interval_mbps.iter().filter(|rate| **rate <= 0.0).count();
    if zero_intervals != 0 {
        return Err(format!(
            "direct TUIC probe has {zero_intervals} zero-rate intervals"
        ));
    }
    let receiver_mbps = value
        .get("end")
        .and_then(|end| end.get("sum_received"))
        .and_then(|sum| sum.get("bits_per_second"))
        .and_then(serde_json::Value::as_f64)
        .map(|bits_per_second| bits_per_second / 1_000_000.0)
        .ok_or_else(|| "direct TUIC probe iperf3 JSON has no receiver rate".to_string())?;
    if receiver_mbps <= floor_mbps {
        return Err(format!(
            "direct TUIC probe receiver {receiver_mbps:.3} <= floor {floor_mbps:.3}"
        ));
    }
    Ok(IperfProbeResult {
        receiver_mbps,
        intervals: interval_mbps.len(),
        zero_intervals,
        min_interval_mbps: interval_mbps.into_iter().reduce(f64::min).unwrap_or(0.0),
    })
}

fn validate_timed_capacity_probe(
    raw: &str,
    expected_intervals: usize,
    floor_mbps: f64,
    bridge: &BridgeResult,
) -> Result<TimedCapacityProbeResult, String> {
    let iperf = validate_reverse_iperf3_json(raw, expected_intervals, floor_mbps)?;
    let terminal = match (
        bridge.accepted_connections,
        bridge.completed_connections,
        bridge.terminal_reset_connections,
        bridge.failed_connections,
        bridge.first_terminal_reset.as_deref(),
        bridge.first_failure.as_deref(),
    ) {
        (2, 2, 0, 0, None, None) => TimedCapacityTerminal::Clean,
        (2, 1, 1, 0, Some(_), None) => TimedCapacityTerminal::OneReset,
        _ => {
            return Err(format!(
                "direct TUIC timed-capacity bridge invalid: accepted={} completed={} terminal_resets={} failed={} first_terminal_reset={} first_failure={}",
                bridge.accepted_connections,
                bridge.completed_connections,
                bridge.terminal_reset_connections,
                bridge.failed_connections,
                bridge.first_terminal_reset.as_deref().unwrap_or("none"),
                bridge.first_failure.as_deref().unwrap_or("none")
            ));
        }
    };
    Ok(TimedCapacityProbeResult {
        receiver_mbps: iperf.receiver_mbps,
        intervals: iperf.intervals,
        zero_intervals: iperf.zero_intervals,
        min_interval_mbps: iperf.min_interval_mbps,
        terminal,
    })
}

fn required_probe_env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

fn probe_env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .map_err(|err| format!("invalid {name}={raw:?}: {err}")),
        Err(_) => Ok(default),
    }
}

async fn finish_bridge(
    mut bridge: tokio::task::JoinHandle<Result<BridgeResult, String>>,
    drain_timeout: Duration,
) -> Result<BridgeResult, String> {
    match tokio::time::timeout(drain_timeout, &mut bridge).await {
        Ok(joined) => joined.map_err(|err| format!("direct TUIC probe bridge join: {err}"))?,
        Err(_) => {
            bridge.abort();
            let _ = bridge.await;
            Err(format!(
                "direct TUIC probe bridge drain timed out after {}ms",
                drain_timeout.as_millis()
            ))
        }
    }
}

async fn run_cross_host_direct_tuic_probe_from_env() -> Result<(), String> {
    let target = TargetAddr::parse(&required_probe_env("MINI_VPN_KNIFE14_DIRECT_TUIC_TARGET")?)
        .map_err(|err| format!("direct TUIC probe target: {err:?}"))?;
    let duration_secs = probe_env_u64("MINI_VPN_KNIFE14_DIRECT_TUIC_DURATION_SECS", 20)?;
    let floor_mbps = probe_env_u64("MINI_VPN_KNIFE14_DIRECT_TUIC_FLOOR_MBPS", 150)? as f64;
    let max_connections = probe_env_u64("MINI_VPN_KNIFE14_DIRECT_TUIC_MAX_CONNECTIONS", 8)?;
    if !(1..=32).contains(&max_connections) {
        return Err(format!(
            "direct TUIC probe max connections out of range: {max_connections}"
        ));
    }
    if !(1..=30).contains(&duration_secs) {
        return Err(format!(
            "direct TUIC probe duration out of range: {duration_secs}"
        ));
    }
    if tuic_tcp_relay_mode() != TuicTcpRelayMode::OrderedJoin {
        return Err(format!(
            "direct TUIC probe requires ordered_join, got {}",
            tuic_tcp_relay_mode().as_str()
        ));
    }

    let mut config =
        TuicClientConfig::from_env().map_err(|err| format!("direct TUIC probe config: {err:?}"))?;
    config.congestion_control = "cubic".to_string();
    config.mtu_policy = "safe1200".to_string();
    config.tcp_pool = 1;
    config.zero_rtt = false;
    config.quic_stats_secs = Some(1);
    let upstream: Arc<dyn ProxyUpstream> = Arc::new(
        TuicUpstream::connect(&config, Arc::new(Metrics::new()))
            .await
            .map_err(|err| format!("direct TUIC probe connect/authenticate: {err:?}"))?,
    );

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|err| format!("direct TUIC probe bind loopback: {err}"))?;
    let listen_addr = listener
        .local_addr()
        .map_err(|err| format!("direct TUIC probe loopback address: {err}"))?;
    println!(
        "direct_tuic_probe_start server={} target={:?} listen={} profile=cubic_safe1200 pool=1 relay=ordered_join duration_secs={} floor_mbps={:.3} max_connections={}",
        config.server, target, listen_addr, duration_secs, floor_mbps, max_connections,
    );

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let bridge = tokio::spawn(run_bounded_loopback_bridge(
        listener,
        upstream,
        target,
        max_connections as usize,
        stop_rx,
    ));

    let mut command = tokio::process::Command::new(
        std::env::var("MINI_VPN_KNIFE14_DIRECT_TUIC_IPERF3")
            .unwrap_or_else(|_| "iperf3".to_string()),
    );
    command
        .arg("-c")
        .arg(listen_addr.ip().to_string())
        .arg("-p")
        .arg(listen_addr.port().to_string())
        .arg("-t")
        .arg(duration_secs.to_string())
        .arg("-P")
        .arg("1")
        .arg("-R")
        .arg("-i")
        .arg("1")
        .arg("--json")
        .kill_on_drop(true);
    let output = tokio::time::timeout(
        Duration::from_secs(duration_secs.saturating_add(30)),
        command.output(),
    )
    .await;
    let _ = stop_tx.send(true);
    let bridge_result = finish_bridge(bridge, Duration::from_secs(5)).await?;
    let output = output
        .map_err(|_| "direct TUIC probe iperf3 timed out".to_string())?
        .map_err(|err| format!("direct TUIC probe run iperf3: {err}"))?;
    let stdout = String::from_utf8(output.stdout)
        .map_err(|err| format!("direct TUIC probe iperf3 stdout is not UTF-8: {err}"))?;
    if let Ok(path) = std::env::var("MINI_VPN_KNIFE14_DIRECT_TUIC_IPERF_JSON_OUT") {
        std::fs::write(&path, &stdout)
            .map_err(|err| format!("direct TUIC probe write iperf3 JSON {path:?}: {err}"))?;
    }
    println!(
        "direct_tuic_probe_bridge accepted_connections={} completed_connections={} terminal_reset_connections={} failed_connections={} local_to_remote_bytes={} remote_to_local_bytes={} first_terminal_reset={} first_failure={}",
        bridge_result.accepted_connections,
        bridge_result.completed_connections,
        bridge_result.terminal_reset_connections,
        bridge_result.failed_connections,
        bridge_result.local_to_remote_bytes,
        bridge_result.remote_to_local_bytes,
        bridge_result
            .first_terminal_reset
            .as_deref()
            .unwrap_or("none"),
        bridge_result.first_failure.as_deref().unwrap_or("none"),
    );
    if !output.status.success() {
        return Err(format!(
            "direct TUIC probe iperf3 failed status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let result =
        validate_timed_capacity_probe(&stdout, duration_secs as usize, floor_mbps, &bridge_result)?;
    println!(
        "direct_tuic_probe_result receiver_mbps={:.3} intervals={} zero_intervals={} min_interval_mbps={:.3} terminal={} verdict=PASS",
        result.receiver_mbps,
        result.intervals,
        result.zero_intervals,
        result.min_interval_mbps,
        result.terminal.as_str(),
    );
    Ok(())
}

async fn relay_one_connection(
    mut local: TcpStream,
    upstream: Arc<dyn ProxyUpstream>,
    target: TargetAddr,
) -> Result<(u64, u64), BridgeFailure> {
    let mut remote = upstream.open_tcp(&target).await.map_err(|err| {
        BridgeFailure::unexpected(format!("direct TUIC probe open target {target:?}: {err:?}"))
    })?;
    tokio::io::copy_bidirectional(&mut local, &mut remote)
        .await
        .map_err(|err| BridgeFailure::relay_io(&target, err))
}

async fn run_bounded_loopback_bridge(
    listener: TcpListener,
    upstream: Arc<dyn ProxyUpstream>,
    target: TargetAddr,
    max_connections: usize,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<BridgeResult, String> {
    if max_connections == 0 {
        return Err("direct TUIC probe max_connections must be nonzero".to_string());
    }
    let mut result = BridgeResult::default();
    let mut relays = JoinSet::new();
    let mut stopping = false;
    loop {
        if stopping && relays.is_empty() {
            return Ok(result);
        }
        tokio::select! {
            changed = stop.changed(), if !stopping => {
                if changed.is_err() || *stop.borrow() {
                    stopping = true;
                }
            }
            accepted = listener.accept(), if !stopping && relays.len() < max_connections => {
                let (local, _) = accepted
                    .map_err(|err| format!("direct TUIC probe accept loopback: {err}"))?;
                result.accepted_connections += 1;
                relays.spawn(relay_one_connection(
                    local,
                    upstream.clone(),
                    target.clone(),
                ));
            }
            joined = relays.join_next(), if !relays.is_empty() => {
                let relay_result = joined
                    .ok_or_else(|| "direct TUIC probe relay set ended unexpectedly".to_string())?
                    .map_err(|err| format!("direct TUIC probe relay task join: {err}"))?;
                match relay_result {
                    Ok((local_to_remote, remote_to_local)) => {
                        result.completed_connections += 1;
                        result.local_to_remote_bytes = result
                            .local_to_remote_bytes
                            .saturating_add(local_to_remote);
                        result.remote_to_local_bytes = result
                            .remote_to_local_bytes
                            .saturating_add(remote_to_local);
                    }
                    Err(failure) => {
                        match failure.kind {
                            BridgeFailureKind::TerminalReset => {
                                result.terminal_reset_connections += 1;
                                if result.first_terminal_reset.is_none() {
                                    result.first_terminal_reset = Some(failure.message);
                                }
                            }
                            BridgeFailureKind::Unexpected => {
                                result.failed_connections += 1;
                                if result.first_failure.is_none() {
                                    result.first_failure = Some(failure.message);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

struct ScriptedUpstream;

#[async_trait::async_trait]
impl ProxyUpstream for ScriptedUpstream {
    async fn open_tcp(&self, _target: &TargetAddr) -> Result<RelayStream, ClientError> {
        let (near, mut far) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            let mut request = Vec::new();
            far.read_to_end(&mut request).await.unwrap();
            far.write_all(b"remote:").await.unwrap();
            far.write_all(&request).await.unwrap();
            far.shutdown().await.unwrap();
        });
        Ok(Box::new(near))
    }
}

struct FailFirstUpstream {
    opens: AtomicUsize,
}

#[async_trait::async_trait]
impl ProxyUpstream for FailFirstUpstream {
    async fn open_tcp(&self, _target: &TargetAddr) -> Result<RelayStream, ClientError> {
        if self.opens.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ClientError::InvalidTarget("injected open failure".into()));
        }
        ScriptedUpstream.open_tcp(_target).await
    }
}

async fn run_probe_client(addr: SocketAddr, payload: &'static [u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(payload).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    response
}

fn expected_injected_reset(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
    )
}

async fn run_expected_failure_client(addr: SocketAddr, payload: &'static [u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    if let Err(err) = stream.write_all(payload).await {
        assert!(
            expected_injected_reset(&err),
            "unexpected write error: {err}"
        );
    }
    if let Err(err) = stream.shutdown().await {
        assert!(
            expected_injected_reset(&err),
            "unexpected shutdown error: {err}"
        );
    }
    let mut response = Vec::new();
    if let Err(err) = stream.read_to_end(&mut response).await {
        assert!(
            expected_injected_reset(&err),
            "unexpected read error: {err}"
        );
    }
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_loopback_bridge_relays_concurrent_connections_and_half_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let bridge = tokio::spawn(run_bounded_loopback_bridge(
        listener,
        Arc::new(ScriptedUpstream),
        TargetAddr::IpPort("127.0.0.1:5201".parse().unwrap()),
        4,
        stop_rx,
    ));

    let (first, second) = tokio::join!(
        run_probe_client(addr, b"first-payload"),
        run_probe_client(addr, b"second-payload"),
    );
    assert_eq!(first, b"remote:first-payload");
    assert_eq!(second, b"remote:second-payload");
    stop_tx.send(true).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), bridge)
        .await
        .expect("bridge must stop after all relays drain")
        .unwrap()
        .unwrap();
    assert_eq!(result.accepted_connections, 2);
    assert_eq!(result.completed_connections, 2);
    assert_eq!(result.local_to_remote_bytes, 27);
    assert_eq!(result.remote_to_local_bytes, 41);
}

#[tokio::test]
async fn bridge_drain_timeout_aborts_and_reaps_the_task() {
    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let signal = DropSignal(dropped.clone());
    let bridge = tokio::spawn(async move {
        let _signal = signal;
        std::future::pending::<Result<BridgeResult, String>>().await
    });
    let result = finish_bridge(bridge, Duration::from_millis(10)).await;
    assert!(result.is_err());
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_relay_failure_does_not_abort_other_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let bridge = tokio::spawn(run_bounded_loopback_bridge(
        listener,
        Arc::new(FailFirstUpstream {
            opens: AtomicUsize::new(0),
        }),
        TargetAddr::IpPort("127.0.0.1:5201".parse().unwrap()),
        4,
        stop_rx,
    ));

    let first = run_expected_failure_client(addr, b"first-will-fail").await;
    assert!(first.is_empty());
    let second = run_probe_client(addr, b"second-survives").await;
    assert_eq!(second, b"remote:second-survives");
    stop_tx.send(true).unwrap();

    let result = finish_bridge(bridge, Duration::from_secs(2)).await.unwrap();
    assert_eq!(result.accepted_connections, 2);
    assert_eq!(result.completed_connections, 1);
    assert_eq!(result.failed_connections, 1);
}

#[test]
fn reverse_iperf_gate_requires_floor_and_every_interval_to_carry_data() {
    let passing = r#"{
        "intervals":[
            {"sum":{"bits_per_second":181000000.0}},
            {"sum":{"bits_per_second":179000000.0}}
        ],
        "end":{"sum_received":{"bits_per_second":180000000.0}}
    }"#;
    let result = validate_reverse_iperf3_json(passing, 2, 150.0).unwrap();
    assert_eq!(result.receiver_mbps, 180.0);
    assert_eq!(result.intervals, 2);
    assert_eq!(result.zero_intervals, 0);
    assert_eq!(result.min_interval_mbps, 179.0);

    let zero_interval = passing.replace("179000000.0", "0.0");
    assert!(validate_reverse_iperf3_json(&zero_interval, 2, 150.0).is_err());
    let below_floor = passing.replace("180000000.0", "150000000.0");
    assert!(validate_reverse_iperf3_json(&below_floor, 2, 150.0).is_err());
    assert!(validate_reverse_iperf3_json(passing, 3, 150.0).is_err());
}

#[test]
fn timed_capacity_gate_accepts_complete_iperf_after_one_terminal_reset() {
    let shoes_capacity = r#"{
        "intervals":[
            {"sum":{"bits_per_second":288060000.0}},
            {"sum":{"bits_per_second":184551000.0}},
            {"sum":{"bits_per_second":185599000.0}},
            {"sum":{"bits_per_second":174063000.0}},
            {"sum":{"bits_per_second":205634000.0}},
            {"sum":{"bits_per_second":186544000.0}},
            {"sum":{"bits_per_second":188744000.0}},
            {"sum":{"bits_per_second":187696000.0}},
            {"sum":{"bits_per_second":181403000.0}},
            {"sum":{"bits_per_second":193987000.0}},
            {"sum":{"bits_per_second":187706000.0}},
            {"sum":{"bits_per_second":187695000.0}},
            {"sum":{"bits_per_second":187683000.0}},
            {"sum":{"bits_per_second":181404000.0}},
            {"sum":{"bits_per_second":193987000.0}},
            {"sum":{"bits_per_second":153099296.0}},
            {"sum":{"bits_per_second":222287000.0}},
            {"sum":{"bits_per_second":187707000.0}},
            {"sum":{"bits_per_second":185587000.0}},
            {"sum":{"bits_per_second":189791000.0}}
        ],
        "end":{"sum_received":{"bits_per_second":192665956.0}}
    }"#;
    let bridge = BridgeResult {
        accepted_connections: 2,
        completed_connections: 1,
        terminal_reset_connections: 1,
        failed_connections: 0,
        local_to_remote_bytes: 513,
        remote_to_local_bytes: 344,
        first_terminal_reset: Some(
            "direct TUIC probe relay IpPort(43.130.32.77:5201): Connection reset by peer (os error 104)"
                .to_string(),
        ),
        first_failure: None,
    };

    let result = validate_timed_capacity_probe(shoes_capacity, 20, 150.0, &bridge).unwrap();
    assert_eq!(result.receiver_mbps, 192.665956);
    assert_eq!(result.intervals, 20);
    assert_eq!(result.zero_intervals, 0);
    assert_eq!(result.min_interval_mbps, 153.099296);
    assert_eq!(result.terminal, TimedCapacityTerminal::OneReset);
}

#[test]
fn timed_capacity_gate_rejects_open_failure_even_when_text_mentions_reset() {
    let complete_iperf = r#"{
        "intervals":[{"sum":{"bits_per_second":180000000.0}}],
        "end":{"sum_received":{"bits_per_second":180000000.0}}
    }"#;
    let bridge = BridgeResult {
        accepted_connections: 2,
        completed_connections: 1,
        terminal_reset_connections: 0,
        failed_connections: 1,
        local_to_remote_bytes: 0,
        remote_to_local_bytes: 0,
        first_terminal_reset: None,
        first_failure: Some(
            "direct TUIC probe open target: transport Connection reset by peer".to_string(),
        ),
    };

    assert!(validate_timed_capacity_probe(complete_iperf, 1, 150.0, &bridge).is_err());
}

#[test]
fn timed_capacity_gate_rejects_terminal_reset_without_preserved_cause() {
    let complete_iperf = r#"{
        "intervals":[{"sum":{"bits_per_second":180000000.0}}],
        "end":{"sum_received":{"bits_per_second":180000000.0}}
    }"#;
    let bridge = BridgeResult {
        accepted_connections: 2,
        completed_connections: 1,
        terminal_reset_connections: 1,
        failed_connections: 0,
        local_to_remote_bytes: 0,
        remote_to_local_bytes: 0,
        first_terminal_reset: None,
        first_failure: None,
    };

    assert!(validate_timed_capacity_probe(complete_iperf, 1, 150.0, &bridge).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run explicitly on the Knife14 VPS path"]
async fn cross_host_direct_tuic_connect_reverse_probe() {
    run_cross_host_direct_tuic_probe_from_env().await.unwrap();
}
