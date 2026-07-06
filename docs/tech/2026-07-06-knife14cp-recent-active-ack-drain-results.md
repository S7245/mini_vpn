# Knife14cp Recent-Active ACK Drain Results

Date: 2026-07-06

## Code

- Commit: `5a3f4e9`
- Scope: after accepted or still-pending downlink work, keep a short
  recent-active deadline so the 5ms timer can run a small TUN RX ACK/window
  drain below the pressure edge. Pressure maintenance still has priority.

## Local Gates

- `cargo test recent_active --lib`
- `cargo test tun_rx_drain --lib`
- `cargo test downlink_backpressure --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test close --lib`
- `cargo test reap --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib` (`328` passed)
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Attempts

Setup failure:

- Bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_20260706_035349/mvpn_knife14cp_recent_active_timer_usclient_suite_20260707_035349.tar.gz`
- Cause: the suite was run with `SERVER_EVIDENCE_CHECK=0`, but the exit-side
  baseline check still required explicit `EXIT_SSH_HOST` and `EXIT_SSH_KEY`.
  This was a test setup failure before P1, not mini_vpn behavior.

Valid scoped run:

- Bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_retry_20260706_035508/mvpn_knife14cp_recent_active_timer_retry_usclient_suite_20260707_035508.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_retry_20260706_035508_local/`
- Remote source hash:
  `80a9045d64fba6f5619cbbc023b7714c2e6f132b11ba7fcb212ed14df9af2579`
- Remote binary hash:
  `970dd7cec5d9b339f88147569c10ac474a624e80c49668131f3e8de8e8d2fa4a`
- The run disabled server evidence and set explicit exit SSH variables, so it
  avoided the post-probe `.77:22` tunnel pollution seen in Knife14co.

Direct baselines were healthy:

- `.27 -> .77`: `331/286 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `308/275 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `312/260 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `306/282 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 regressed:

- `iperf_sender_mbps=19.200`
- `iperf_receiver_mbps=18.000`
- `throughput_shape=low_average local_pressure=0 no_data=0`

## Clean Signals

- The run was not a no-data branch.
- The `.27` process and route cleanup succeeded after the run.
- Current `.33` log inspection showed no TUIC `fail auth`.
- Local pressure stayed clean during the P1 attribution window:
  `downlink_backpressure pause_edges=0`, `tun_tx_dropped_delta=0`,
  runtime `drop_delta_total=0`.
- Pending/close/reap accounting stayed clean:
  `pending_at_close=0`, `egress_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`.
- QUIC loss/congestion/blocking deltas stayed zero.
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`.

## Failure Signals

- Throughput was worse than Knife14co:
  `18.0 Mbit/s` receiver versus `24.3 Mbit/s`.
- The run remained burst/idle:
  `overall_avg_mbps=18.004`, `tail_avg_mbps=5.950`, with many zero-throughput
  one-second intervals.
- The new timer path definitely engaged:
  `timer_active_flow_attempts=1321`.
- Total TUN RX drain work increased:
  `tun_rx_drain attempts=10633 packets=18591 tcp=18591 budget_exhausted=91
  would_block=10542 errors=0`.
- `send_queue_max=561658`, still below the credit edge but higher than
  Knife14co's `427496`.
- Attribution remained `no_pressure_signal`, so the added timer drain did not
  address the remaining bottleneck.

## Interpretation

Knife14cp rejects recent-active timer ACK/window drain as a product default.
The hypothesis was not under-tested: diagnostics proved the timer source ran
more than a thousand times, pressure and close/reap surfaces stayed clean, and
the direct path was healthy. The result still regressed materially versus
Knife14co.

The remaining Knife14 root is therefore not "more below-pressure ACK drain".
The next stage should restore the cleaner Knife14co default behavior and move
to the no-pressure burst/idle branch: stream scheduling, wake/read cadence, or
remote-to-local delivery timing that can leave reverse throughput idle even
when local TUN drops, pending close, and QUIC congestion are absent.

## Next

Disable or remove recent-active timer drain from the default path while keeping
the stage evidence. Then continue with a focused no-pressure burst/idle spec
that adds diagnostics or behavior at the TUIC stream read/wake boundary rather
than adding another TUN RX drain variant.
