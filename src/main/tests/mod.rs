//! Existing command-line probe characterization tests; no runtime behavior is owned here.
//! The explicit crate-root seam preserves the original test names and assertions.
//! All moved tests remain in this private module.

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
