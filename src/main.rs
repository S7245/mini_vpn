use mini_vpn::client_tun;
use mini_vpn::metrics::Metrics;
use mini_vpn::reality_upstream;
use mini_vpn::shared::{ClientError, TargetAddr};
use mini_vpn::tuic::{TuicClientConfig, TuicUpstream};
use mini_vpn::upstream::ProxyUpstream;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone, PartialEq)]
struct TuicTcpSinkProbeReport {
    target: String,
    requested_duration_secs: u64,
    elapsed_ms: u128,
    bytes: u64,
    reads: u64,
    first_rx_ms: Option<u128>,
    max_read_gap_ms: u128,
    eof: bool,
}

#[tokio::main]
async fn main() {
    // Stage 13d 起退役 legacy（自研 server / 直连 client / yamux），仅保留 TUN + TUIC 客户端。
    match std::env::args().nth(1).as_deref() {
        Some("client") | Some("client-tun") | None => client_tun::start_tun_proxy().await,
        // 刀8 诊断：直连探针（无 TUN/无 sudo），用 MINI_VPN_REALITY_* env 跑一次 REALITY 握手 + HTTP GET。
        // 用法：MINI_VPN_REALITY_*=... [MINI_VPN_REALITY_DEBUG=1] ./mini_vpn reality-probe [host:80]
        Some("reality-probe") => {
            let target = std::env::args()
                .nth(2)
                .unwrap_or_else(|| "example.com:80".into());
            reality_upstream::reality_probe(&target).await;
        }
        Some("tuic-tcp-sink-probe") => {
            let mut args = std::env::args().skip(2);
            let target = args.next().unwrap_or_else(|| {
                eprintln!("usage: mini_vpn tuic-tcp-sink-probe <host:port> [duration_secs]");
                std::process::exit(2);
            });
            let duration_secs = match args.next() {
                Some(raw) => match parse_probe_duration_secs(&raw) {
                    Ok(secs) => secs,
                    Err(err) => {
                        eprintln!("{err}");
                        std::process::exit(2);
                    }
                },
                None => 30,
            };
            match tuic_tcp_sink_probe(&target, duration_secs).await {
                Ok(report) => println!("{}", format_tuic_tcp_sink_probe_report(&report)),
                Err(err) => {
                    eprintln!("tuic-tcp-sink-probe failed: {err}");
                    std::process::exit(1);
                }
            }
        }
        Some(other) => {
            panic!("未知运行模式: {other}（支持 client-tun | reality-probe | tuic-tcp-sink-probe）")
        }
    }
}

async fn tuic_tcp_sink_probe(
    target: &str,
    duration_secs: u64,
) -> Result<TuicTcpSinkProbeReport, ClientError> {
    let target_addr = TargetAddr::parse(target)?;
    let cfg = TuicClientConfig::from_env()?;
    let metrics = Arc::new(Metrics::new());
    let upstream = TuicUpstream::connect(&cfg, metrics).await?;
    let mut stream = upstream.open_tcp(&target_addr).await?;

    let duration = Duration::from_secs(duration_secs);
    let start = tokio::time::Instant::now();
    let deadline = start + duration;
    let mut buf = vec![0u8; 256 * 1024];
    let mut bytes = 0u64;
    let mut reads = 0u64;
    let mut first_rx_ms = None;
    let mut last_read_at = None;
    let mut max_read_gap_ms = 0u128;
    let mut eof = false;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.duration_since(now);
        let read_result = tokio::time::timeout(remaining, stream.read(&mut buf)).await;
        let n = match read_result {
            Ok(result) => result?,
            Err(_) => break,
        };
        if n == 0 {
            eof = true;
            break;
        }
        let read_at = tokio::time::Instant::now();
        bytes = bytes.saturating_add(n as u64);
        reads = reads.saturating_add(1);
        first_rx_ms.get_or_insert_with(|| read_at.duration_since(start).as_millis());
        if let Some(prev) = last_read_at {
            max_read_gap_ms = max_read_gap_ms.max(read_at.duration_since(prev).as_millis());
        }
        last_read_at = Some(read_at);
    }

    Ok(TuicTcpSinkProbeReport {
        target: target.to_string(),
        requested_duration_secs: duration_secs,
        elapsed_ms: start.elapsed().as_millis(),
        bytes,
        reads,
        first_rx_ms,
        max_read_gap_ms,
        eof,
    })
}

fn parse_probe_duration_secs(raw: &str) -> Result<u64, String> {
    let secs = raw
        .parse::<u64>()
        .map_err(|_| format!("invalid duration_secs={raw:?}; expected integer seconds"))?;
    if !(1..=600).contains(&secs) {
        return Err(format!(
            "invalid duration_secs={secs}; expected range is 1..=600"
        ));
    }
    Ok(secs)
}

fn tuic_tcp_sink_probe_mbps(bytes: u64, elapsed_ms: u128) -> f64 {
    if elapsed_ms == 0 {
        return 0.0;
    }
    (bytes as f64 * 8.0) / (elapsed_ms as f64 / 1000.0) / 1_000_000.0
}

fn format_tuic_tcp_sink_probe_report(report: &TuicTcpSinkProbeReport) -> String {
    format!(
        "tuic_tcp_sink_probe target={} requested_duration_secs={} elapsed_ms={} bytes={} read_mbps={:.3} reads={} first_rx_ms={} max_read_gap_ms={} eof={}",
        report.target,
        report.requested_duration_secs,
        report.elapsed_ms,
        report.bytes,
        tuic_tcp_sink_probe_mbps(report.bytes, report.elapsed_ms),
        report.reads,
        report
            .first_rx_ms
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string()),
        report.max_read_gap_ms,
        report.eof
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_duration_accepts_bounded_seconds() {
        assert_eq!(parse_probe_duration_secs("1").unwrap(), 1);
        assert_eq!(parse_probe_duration_secs("600").unwrap(), 600);
        assert!(parse_probe_duration_secs("0").is_err());
        assert!(parse_probe_duration_secs("601").is_err());
        assert!(parse_probe_duration_secs("abc").is_err());
    }

    #[test]
    fn sink_probe_report_formats_capacity_metrics() {
        let report = TuicTcpSinkProbeReport {
            target: "43.130.32.77:5202".to_string(),
            requested_duration_secs: 10,
            elapsed_ms: 10_000,
            bytes: 125_000_000,
            reads: 2048,
            first_rx_ms: Some(12),
            max_read_gap_ms: 3,
            eof: false,
        };
        let line = format_tuic_tcp_sink_probe_report(&report);
        assert!(line.contains("read_mbps=100.000"));
        assert!(line.contains("first_rx_ms=12"));
        assert!(line.contains("max_read_gap_ms=3"));
        assert!(line.contains("eof=false"));
    }
}
