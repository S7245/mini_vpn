# Knife14cj Pre-Payload Pressure ACK Drain Results

Date: 2026-07-06

## Code

- Commit: `567d25b`
- Scope: pressure-gated TUN RX drain now runs before eligible remote payload
  writes and during timer pressure maintenance; diagnostics distinguish
  pre-payload, remote-payload, maintenance, and other drain attempts.

## Local Gates

- `cargo test tun_rx_drain --lib`
- `cargo test pre_payload --lib`
- `cargo test maintenance --lib`
- `cargo test tun_rx_pressure --lib`
- `cargo test downlink_egress_clock --lib`
- `cargo test pressure_credit --lib`
- `cargo test credit_debt --lib`
- `cargo test drop_credit --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

Note: `cargo fmt --check` was not used as a gate because the repository has
pre-existing rustfmt drift across unrelated files. Running it reports broad
format-only changes outside this stage.

## VPS Runs

Initial run:

- Remote bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_20260707_0229/mvpn_knife14cj_pre_payload_ack_drain_usclient_suite_20260707_022926.tar.gz`
- Outcome: failed before tunnel P1 because `EXIT_TO_TARGET_IPERF_CHECK=1`
  was enabled without `EXIT_SSH_HOST`.

Retry:

- Remote bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_retry_20260707_0231/mvpn_knife14cj_pre_payload_ack_drain_retry_usclient_suite_20260707_023111.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_retry_20260707_0231_local/`

Baselines were healthy:

- `.27 -> .77`: `335/281 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `309/281 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `313/287 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `309/281 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 failed:

- `iperf_sender_mbps=15.500`
- `iperf_receiver_mbps=14.500`
- `throughput_shape=low_average local_pressure=1`

Key clean signals:

- `terminal_pending_reap=0`
- `pending_at_close=0`
- `egress_at_close=0`
- `terminal_late_remote_payload=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- QUIC loss/congestion/blocking deltas remained zero.
- No current `.33` `fail auth` evidence; no-secret config checks showed
  UUID/password/ALPN match and service time was aligned.

Key failure signals:

- `tun_tx_dropped_delta=7794`
- `runtime_tun_egress drop_events=5 drop_delta_total=7056 max_delta=1931`
- `tun_egress_feedback pause_edges=5 resume_edges=4 max_pressure_bytes=892928`
- `downlink_flush send_queue_max=892928`
- `hard_edge_guard_limited=3226`
- `tun_flush_deferred=221`
- `tuic_tcp_stream data_read_gap_max_ms=5584`
- `tuic_stream_pending data_pending_gap_max_ms=5580`

Knife14cj-specific signal:

- Parser summary: `tun_rx_drain attempts=252 packets=5292 tcp=5292
  budget_exhausted=252 would_block=0 errors=0`.
- Raw log with source counters:
  `pre_payload_attempts=41 remote_payload_attempts=200
  maintenance_attempts=11 other_attempts=0`.

## Interpretation

Knife14cj proved that the new pre-payload and maintenance drain paths engage,
but it did not improve throughput. Every pressure drain exhausted its packet
budget and never reached `would_block`, so the pressure-drain budget is
undersized for the actual ready TUN RX workload.

The likely reason is the Knife14ci budget formula: it derives packet count from
`tx_queue_credit_guard_bytes / tun_mtu`. That assumes data-sized packets, but
the pressure-drain target is mostly local TCP ACK/window traffic, which is much
smaller than MTU. With the default guard and MTU 1200, the adaptive budget is
only `21` packets. The VPS run shows 252 consecutive exhausted drains, so the
queue was never fully drained.

## Next

Knife14ck should change the adaptive pressure-drain budget algorithm rather
than setting a larger static environment value. The next budget should be
derived for ACK-sized packets, bounded by a hard cap, and tested so pressure
drain can reach `would_block` without becoming an unconditional TUN RX sweep.
