# Knife14cn Debt-Coupled Receive Gate Results

Date: 2026-07-06

## Code

- Commit: `b6981ad`
- Scope: active drop/pressure credit debt now participates in downlink receive
  backpressure. The no-debt helper remains the original behavior; debt-active
  pressure at the existing credit threshold pauses remote receive before the
  hard pause edge.

## Local Gates

- `cargo test active_credit_debt --lib`
- `cargo test downlink_backpressure --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test pending_downlink_close_deferral --lib`
- `cargo test close --lib`
- `cargo test reap --lib`
- `cargo test tun_rx_drain --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib` (`325` passed)
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Run

The first `.27` launch failed before tunnel startup because the suite command
did not source `.env`:

- Failed bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_20260706_1927/mvpn_knife14c_usclient_suite_20260707_032726.tar.gz`
- Failure:
  `MISSING: MINI_VPN_TUIC_SERVER` and related TUIC variables.

The valid run sourced `.env` first:

- Bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_env_20260706_1930/mvpn_knife14c_usclient_suite_20260707_032833.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_env_20260706_1930_local/`
- Binary hash:
  `acd527579574983987e9eff6233d632c004d3f38f7120bc3cd6be1b60376c16e`

Baselines were healthy:

- `.27 -> .77`: `332/280 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `317/291 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `309/280 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `316/284 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 failed throughput acceptance:

- `iperf_sender_mbps=20.500`
- `iperf_receiver_mbps=19.200`
- `throughput_shape=low_average local_pressure=1 no_data=0`

## Clean Or Improved Signals

- The run was not a no-data branch.
- Direct baselines stayed healthy.
- Current `.33` checks still showed no TUIC `fail auth`.
- QUIC loss/congestion/blocking deltas stayed zero.
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `terminal_pending_reap=0`
- `terminal_late_remote_payload=0`
- `pending_at_close=0`
- `egress_at_close=0`

Knife14cn-specific improvement:

- `tcp-egress-credit-debt reason=pressure_credit_edge installed_bytes=24576`
  was immediately followed by `tcp-downlink-backpressure paused=true` at
  `max_pending=2035 max_tx_queue=892928`.
- Final pending stayed tiny compared with Knife14cm:
  `pending_total=2035` versus Knife14cm `pending_total=541802`.
- The parser's final close/reap accounting stayed clean:
  `pending_at_close=0`, `egress_at_close=0`, and `terminal_pending_reap=0`.

## Remaining Failure Signals

- Throughput stayed in the low band at `20.5/19.2 Mbit/s`.
- TUN egress still dropped:
  parser `tun_tx_dropped_delta=3524`; runtime feedback
  `drop_delta_total=2714`.
- Feedback paused and did not resume:
  `pause_edges=1 resume_edges=0`.
- Active debt still did not get paid:
  `pressure_credit_debt_bytes=24576`,
  `pressure_credit_debt_paid_bytes=0`,
  `drop_credit_debt_bytes=172032`.
- Local egress still reached the credit edge:
  `send_queue_max=892928`.
- `may_recv_false=5175`, `headroom_limited=5268`, and
  `headroom_deferred_bytes=12344297`.

## Interpretation

Knife14cn fixed the specific Knife14cm failure mode where receive continued
after credit-edge debt and pending grew into hundreds of KiB. The new
`tcp-downlink-backpressure paused=true` line at `pending=2035` proves active
debt now gates remote receive.

This is still not acceptance. The throughput collapse now occurs with mostly
zero pending during the main window, long remote-read gaps, little TUN RX drain
until late pressure, and a final TUN egress drop at the credit edge. The
remaining branch is no longer close/reap hiding or receive continuing after
debt. The next likely root is ACK/TUN-RX/egress progress cadence before the
credit edge: the sender is not kept continuously open even though local
pending is bounded and QUIC is clean.

## Next

Knife14co should test and repair pre-pressure progress cadence rather than
only adding more debt or lowering thresholds. A focused next hypothesis is that
reverse TCP needs bounded TUN RX ACK/window draining while the data stream is
active, not only when local egress is already near pressure. The next patch
should keep this bounded and observable, with tests proving it does not become
unconditional polling.
