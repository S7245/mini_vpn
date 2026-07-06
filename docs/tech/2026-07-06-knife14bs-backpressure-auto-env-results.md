# Knife14bs Backpressure Auto Env Results

## Code Result

Commit `dd9c6ad` added suite-side protection for stale inherited downlink
backpressure watermarks. The US-client suite now normalizes the legacy
`524288/131072` pair back to `<auto>` under a larger TCP tx buffer unless
`KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=1` is set.

Local gates passed:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`.
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`.
- `bash -n scripts/knife14b-lowrtt-probe.sh`.
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`.
- `cargo test --lib parse_downlink_backpressure_config_scales_defaults_with_tcp_tx_buffer`.
- `cargo test --lib downlink_backpressure_uses_tx_queue_pressure_when_pending_is_empty`.
- `git diff --check`.

## VPS Run 1

Bundle:

- `/tmp/mini_vpn/knife14bs_autoenv_20260706/mvpn_knife14bs_autoenv_dd9c6ad_usclient_suite_20260706_171224.tar.gz`

Outcome:

- The suite correctly reported
  `downlink_backpressure_auto_reset=legacy_512k_for_scaled_tx_buffer`.
- mini_vpn startup log showed the intended auto-scaled receive-window:
  `high=1048576B low=262144B`.
- The run did not reach throughput acceptance because client startup failed:
  `tuic auth finish: sending stopped by peer: error 0`.

No-secret TUIC/auth diagnostics:

- `.27` and `.33` time skew: `0s`.
- `.33` sing-box service: active, UDP `:8443` listening, config check passed.
- Manual no-secret config match from `.27` to `.33`: `uuid_match=1`,
  `password_match=1`, `sni_match=1`, `alpn_match=1`.
- `.33` journal for the failure window had no new sing-box entries.

Interpretation: this was not a stable credential, SNI/ALPN, clock, or service
state mismatch. It was treated as a startup transient and retried once without
changing code or `.33` service state.

## VPS Run 2

Bundle:

- `/tmp/mini_vpn/knife14bs_autoenv_retry_20260706/mvpn_knife14bs_autoenv_retry_dd9c6ad_usclient_suite_20260706_171532.tar.gz`

Local extracted bundle:

- `/tmp/mini_vpn/knife14bs_autoenv_retry_20260706_171532/`

Config evidence:

- suite reported
  `downlink_backpressure_auto_reset=legacy_512k_for_scaled_tx_buffer`;
- mini_vpn startup log showed `high=1048576B low=262144B`;
- TCP socket buffers were `rx=1048576B tx=1048576B`;
- TUN qlen remained the OS default `500`.

Preflight path health:

- `.27 -> .77`: `335/277 Mbit/s`.
- `.27 <- .77`: `323/295 Mbit/s`.
- `.33 -> .77`: `318/287 Mbit/s`.
- `.33 <- .77`: `323/297 Mbit/s`.

Reverse-first P1 result:

- tunnel sender: `72.1 MBytes / 20.2 Mbit/s`;
- tunnel receiver: `67.1 MBytes / 18.8 Mbit/s`;
- throughput shape: `low_average`;
- several one-second intervals were `0.00 bits/sec`;
- target `.77` sender journal matched the same low/zero-window shape.

mini_vpn signals:

- `downlink_backpressure pause/resume=19/19`.
- `max_tx_queue_bytes=1048576`.
- `tun_flush_deferred=19`.
- `tun_tx_dropped_delta=586`.
- `tun_egress_feedback pause/resume=1/1`.
- `terminal_pending_reap=0`.
- `pending_at_close=0`.
- `terminal_late_remote_payload=4180801B` across `82` events.
- QUIC loss/congestion/blocking deltas: all `0`.
- data stream remote read gaps reached `3782ms`;
- data stream pending gaps reached `3782ms`;
- global_rx pressure was `0`, with queue high-water `47/1024`.

## Comparison With Knife14bq

Knife14bq (`d5d8542`, legacy `524288/131072`) had:

- tunnel receiver: `149 Mbit/s`;
- final-tail collapse to about `16 Mbit/s`;
- `downlink_backpressure=693/693`;
- `tun_tx_dropped_delta=14804`;
- `tun_flush_deferred=693`;
- `terminal_late_remote_payload=1474528B`.

Knife14bs auto high/low (`dd9c6ad`, `1048576/262144`) had:

- tunnel receiver: `18.8 Mbit/s`;
- low-average stop/go shape across the whole 30s window;
- `downlink_backpressure=19/19`;
- `tun_tx_dropped_delta=586`;
- `tun_flush_deferred=19`;
- `terminal_late_remote_payload=4180801B`.

The suite fix succeeded, but the behavior result failed.

## Interpretation

The stale-env hypothesis was useful but not sufficient:

- It proved the acceptance suite could silently bypass binary auto defaults.
- It also proved that tx-buffer-scaled high watermark is not safe for this
  topology when the TUN qlen remains `500`.

The remaining bottleneck is still local TCP downlink / TUN egress lifecycle, but
the specific branch is sharper now:

- smoltcp `send_queue` reaches the 1 MiB high watermark;
- the main loop pauses `global_rx`;
- after poll/flush, observable smoltcp pressure can drop to zero while packets
  are already in the Linux TUN/qdisc path;
- remote stream read gaps grow into multi-second stalls;
- target sender cwnd collapses and emits periodic zero-throughput intervals;
- close-tail accounting remains visible and not hidden as app-owned pending.

Do not continue by increasing the receive window or TUN queue as a blind fix.
Knife14bt should design a TUN-egress-capacity-aware downlink pressure model or a
bounded A/B around receive-window high/low that includes qdisc/drop evidence and
stream read-gap acceptance.

## Next Plan Proposal

Before another behavior patch:

1. Add focused accounting for downlink pause duration and stream read-gap
   correlation.
2. Add a deterministic test that prevents `downlink_pressure_stats` from
   reporting a clean resume immediately after a high-pressure flush when a
   TUN-egress pressure latch is active.
3. Re-evaluate the binary auto default: high should not blindly scale to the TCP
   tx buffer when the local TUN egress path cannot absorb that burst.
4. Only after that TDD slice, make the next small behavior patch and run one
   scoped reverse-first P1.
