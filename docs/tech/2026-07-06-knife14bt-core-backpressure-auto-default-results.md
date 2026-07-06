# Knife14bt Core Backpressure Auto Default Results

## Summary

Commit `b5752c4` restored the core auto downlink backpressure default to the
conservative `524288/131072` pair. Local tests passed, and the scoped VPS
acceptance confirmed the startup value was applied, but reverse-first P1 still
failed with a low-average stop/go shape.

## Local Gates

- Red/green TDD:
  - `cargo test --lib client_tun::tests::parse_downlink_backpressure_config_keeps_safe_defaults_with_large_tcp_tx_buffer`
- Focused checks:
  - `cargo test --lib client_tun::tests::downlink_backpressure`
  - `cargo test --lib client_tun::tests::tun_egress_feedback`
  - `cargo test --lib client_tun::tests::parse_downlink_backpressure_config`
- Regression:
  - `cargo test --lib`
  - `git diff --check`

All passed.

## VPS Runs

First run:

- Out dir: `/tmp/mini_vpn/knife14bt_core_auto_20260706`
- Bundle:
  `/tmp/mini_vpn/knife14bt_core_auto_20260706/mvpn_knife14bt_core_auto_usclient_suite_20260706_173954.tar.gz`
- Result: mini_vpn exited during TUIC startup with
  `tuic auth finish: sending stopped by peer: error 0`.
- Diagnostics: `.27/.33` time skew `0s`, `.33` sing-box active, UDP `:8443`
  listening, config check passed. The generated no-secret compare returned a
  `CalledProcessError`, but a manual no-secret compare immediately afterward
  showed `uuid_match=1`, `password_match=1`, `sni_match=1`, and
  `alpn_match=1`.
- Decision: treat as the known one-shot TUIC startup transient and retry once.

Retry:

- Out dir: `/tmp/mini_vpn/knife14bt_core_auto_retry_20260706`
- Bundle:
  `/tmp/mini_vpn/knife14bt_core_auto_retry_20260706/mvpn_knife14bt_core_auto_retry_usclient_suite_20260706_174152.tar.gz`
- Local extracted copy:
  `/tmp/mini_vpn/knife14bt_core_auto_retry_20260706_local/`
- Commit on `.27`: `b5752c41`
- Suite config: downlink high/low `<auto>`
- Startup log: `high=524288B low=131072B`

Direct baselines were healthy:

- `.27 -> .77`: `323/280 Mbit/s`
- `.27 <- .77`: `329/299 Mbit/s`
- `.33 -> .77`: `321/297 Mbit/s`
- `.33 <- .77`: `310/282 Mbit/s`

Tunnel reverse-first P1 failed:

- sender: `66.1 MBytes / 18.4 Mbit/s`
- receiver: `62.1 MBytes / 17.4 Mbit/s`
- `throughput_shape: shape=low_average`
- interval profile: repeated zero-throughput seconds with bursts around
  `57-116 Mbit/s`

## Key Signals

- `downlink_backpressure=26/26`
- `max_tx_queue_bytes=586083`
- `tun_tx_dropped_delta=172`
- `tun_egress_feedback=1/1`
- `tun_flush_deferred=21` in probe summary; the data handle close line recorded
  `tun_flush_deferred=26`
- `terminal_pending_reap=0`
- `terminal_late_remote_payload=2890364B` across `145` events
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- QUIC loss/congestion/blocking deltas all `0`
- global rx pressure events `0`, queue max `12/1024`
- data stream read and pending gaps reached `3873ms`
- `.77` iperf sender journal showed the same stop/go pattern.
- `.33` sing-box had current TUIC inbound/direct outbound entries and no
  current TUIC `fail auth` signal in the collected evidence.

## Interpretation

Knife14bt accepted the core-default correction but did not improve the
reverse-first throughput. The rejected branch is now narrower:

- The low-average shape is not caused only by the unsafe `1048576/262144` auto
  default.
- The run still has no QUIC loss/congestion/blocking evidence.
- The run still has no hidden terminal pending reap or send-slice failure.
- The repeated pattern remains local: smoltcp `send_queue` crosses high,
  downlink reading pauses, pressure immediately appears as zero after poll/flush,
  and the remote stream develops multi-second read/pending gaps.

## Next Slice

Do not continue with scripts, stale pool slots, sing-box, iperf3, TUN qlen, or
larger high/low tuning from this result.

Knife14bu should add a deterministic core test and minimal behavior change for
durable local egress pressure, specifically preventing the downlink gate from
immediately treating a just-flushed high `send_queue` burst as a clean resume
while local TUN/qdisc egress pressure may still be outstanding.
