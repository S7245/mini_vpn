# Knife14co Active-Flow ACK Drain Results

Date: 2026-07-06

## Code

- Commit: `3a3bdbf`
- Scope: default TUN RX ACK/window drain now has a small active-flow budget
  below the tx-queue credit edge, while the pressure-edge path keeps the larger
  ACK-sized pressure budget.

## Local Gates

- `cargo test active_flow --lib`
- `cargo test tun_rx_drain --lib`
- `cargo test downlink_backpressure --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test pending_downlink_close_deferral --lib`
- `cargo test close --lib`
- `cargo test reap --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib` (`327` passed)
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

Note: an extra `cargo fmt --check` was tried and failed on broad pre-existing
formatting differences outside this stage. No formatting rewrite was applied.

## VPS Run

- Bundle:
  `/tmp/mini_vpn/knife14co_active_flow_ack_20260706_034114/mvpn_knife14co_active_flow_ack_usclient_suite_20260707_034114.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14co_active_flow_ack_20260706_034114_local/`
- Remote source hash:
  `104309c7f07a6b0e38b31fee7c877a3e0ab85d3fb5f27fea7485da651c4e3392`
- Remote binary hash:
  `925d86df2fa91df25cd16af73d47f55aad73d86d04b5b168a79eb7ce5ccfa13a`

Direct baselines were healthy:

- `.27 -> .77`: `316/280 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `303/273 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `313/279 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `321/296 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 still failed throughput acceptance:

- `iperf_sender_mbps=25.500`
- `iperf_receiver_mbps=24.300`
- `throughput_shape=low_average local_pressure=0 no_data=0`

## Improved Signals

- The run was not a no-data branch.
- Throughput improved versus Knife14cn (`24.3` receiver versus `19.2`
  Mbit/s), but not enough for acceptance.
- Local pressure disappeared during the P1 attribution window:
  `downlink_backpressure pause_edges=0`, `tun_tx_dropped_delta=0`,
  `drop_delta_total=0`.
- `send_queue_max` dropped to `427496`, below the credit edge.
- Pending/close/reap stayed clean:
  `pending_at_close=0`, `egress_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`.
- Active-flow drain did real work:
  `tun_rx_drain attempts=7506 packets=24055 tcp=24055
  budget_exhausted=15 would_block=7491`.
- QUIC loss/congestion/blocking stayed zero.
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`.
- A separate current `.33` log check after the run showed no `fail auth`.

## Remaining Failure Signals

- Average throughput remained low and bursty:
  `overall_avg_mbps=24.275`, `tail_avg_mbps=10.658`, with many zero-throughput
  one-second intervals.
- The probe attribution was `no_pressure_signal`, not local pressure.
- TUIC stream read/pending gaps still appeared while local egress looked
  healthy: example `max_remote_read_gap_ms=3452` and stream pending/read-gap
  lines for the reverse flow.
- `global_rx_queue_used_max=51` stayed bounded and local loop active time was
  low, so this is not obvious main-loop CPU saturation.

## Evidence Caveat

The suite's server-evidence step opens SSH to `.77` while the `.77/32` route is
still pointed at the TUN. That creates an extra `43.130.32.77:22` tunnel flow
after the P1 attribution window and pollutes the client log tail with later
pressure/drop lines. Interpret Knife14co using the probe-window attribution
summary, not the post-evidence client-log tail.

## Interpretation

Knife14co fixed the immediate local pressure symptom for the P1 attribution
window: TUN drops and downlink backpressure disappeared, send queue stayed
below the credit edge, and active-flow ACK drain reached `would_block` most of
the time.

The remaining low average is therefore not explained by close/reap hiding,
pending growth, TUN drops, QUIC loss/congestion, or global receive pressure.
The next likely branch is that event-driven active-flow drain still depends on
a remote payload event. After a burst, ACK/window updates may wait in TUN RX
when no next remote payload arrives to trigger another drain, causing the
sender to resume only in bursts.

## Next

Knife14cp should add a bounded, recent-active-flow timer drain: when there has
been recent accepted downlink work, the 5ms timer may run a small active-flow
TUN RX drain even below the pressure edge. It must remain time-bounded,
observable, and off when no recent downlink activity exists.
