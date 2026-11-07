use mini_vpn::client_tun;
use mini_vpn::metrics::Metrics;
use mini_vpn::reality_upstream;
use mini_vpn::tuic::TuicClientConfig;
use mini_vpn::tuic::probe::{self, SinkReport as TuicTcpSinkProbeReport};
use std::sync::Arc;

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
) -> Result<TuicTcpSinkProbeReport, String> {
    let cfg = TuicClientConfig::standalone_from_env().map_err(|error| error.to_string())?;
    let mut owner = probe::prepare_sink(cfg, target, duration_secs, Arc::new(Metrics::new()))
        .map_err(|error| error.to_string())?;
    let observed = owner.run().await;
    if observed.phase == probe::Phase::Complete {
        observed
            .sink
            .ok_or_else(|| "sink completed without its original report".into())
    } else {
        Err(owner.failure().map(ToString::to_string).unwrap_or_else(|| {
            format!(
                "sink lifecycle {:?}, node {:?}",
                observed.phase, observed.node
            )
        }))
    }
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
#[path = "main/tests/mod.rs"]
mod tests;
