# Knife14cq Recent-Active Timer Opt-In Results

Date: 2026-07-06

## Code

- Scope: Knife14cp recent-active timer ACK drain is default-disabled and
  available only through `MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS>0`.
- Default product path keeps Knife14co payload-triggered active-flow drain and
  pressure maintenance drain unchanged.

## Local Gates

- `cargo test recent_active --lib`
- `cargo test tun_rx_active_flow --lib`
- `cargo test tun_rx_drain --lib`
- `cargo test downlink_backpressure --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test close --lib`
- `cargo test reap --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib` (`329` passed)
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

One clippy pass initially failed on a collapsible nested `if`; the code was
collapsed without behavior changes, then focused tests and broad gates passed.

## VPS Run

- Remote bundle:
  `/tmp/mini_vpn/knife14cq_timer_optin_off_20260707_040635/mvpn_knife14cq_timer_optin_off_usclient_suite_20260707_040635.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cq_timer_optin_off_20260707_040635_local/`
- Remote source hash:
  `8578fa896716987a1f236af3c3455853e3954dce411963dd8166e6398cc84938`
- Remote binary hash:
  `040e2ad8760eee58c10d62386502a23d2c5a1471d5e6fcf8e8d57931346b617e`
- Suite shape: reverse-first P1 only, `P=1`, `30s`, MTU `1200`,
  server evidence disabled, explicit `.33` exit SSH variables set for exit
  baselines.

Direct baselines were healthy:

- `.27 -> .77`: `322/282 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `327/299 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `305/252 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `321/297 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 passed:

- `iperf_sender_mbps=182.000`
- `iperf_receiver_mbps=181.000`
- `overall_avg_mbps=180.833`
- `tail_avg_mbps=179.333`
- `tail_min_mbps=162.000`
- `throughput_shape=stable_high tail_collapse=0 local_pressure=1 no_data=0`

## Acceptance Signals

- Startup printed
  `TUN RX active-flow timer drain: 0ms`, proving the default path is disabled.
- Runtime drain diagnostics confirmed no timer-active-flow drain:
  `timer_active_flow_attempts=0`.
- TUN RX drain still did bounded event-driven work:
  `attempts=70430`, `pre_payload_attempts=35215`,
  `remote_payload_attempts=35215`, `maintenance_attempts=0`,
  `packets=227330`, `would_block=70406`, `errors=0`.
- Local drops stayed clean:
  `tun_tx_dropped_delta=0`, `drop_delta_total=0`.
- Backpressure stayed clean:
  `downlink_backpressure pause_edges=0 resume_edges=0`.
- Pending/terminal accounting stayed clean:
  `pending_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`.
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`.
- QUIC loss/congestion/blocking deltas were zero.
- Current `.33` log checks before and after the run showed no TUIC
  `fail auth`.
- `.27` cleanup succeeded after the suite: no active client-tun, target route
  restored through `eth0`, and no `tun0` address remained.

## Remaining Observable Tail

The parser still reported one close-time egress tail:

- `egress_at_close: events=1 bytes=443898`
- `close_egress_class=active_send_capable`
- `close_egress_drain_candidate=true`
- `tcp_state=CloseWait can_send=true may_send=true`

This is not the former terminal-pending hidden loss class. The app pending
queue was empty, terminal reap was zero, terminal late payload was zero, and
the receiver completed a stable high-throughput run. Keep the active
send-capable close-egress tail visible, but do not treat it as the current P1
throughput root from this evidence.

## Interpretation

Knife14cq validates the Knife14cp rejection. The timer path was the harmful
change: when default-disabled, reverse-first P1 jumped from Knife14cp's
`19.2/18.0 Mbit/s` back to stable high throughput at `182/181 Mbit/s`, while
the close/reap and transport surfaces stayed clean.

This means Knife14 can stop the below-pressure timer ACK-drain branch. The next
stage should prove stability with a repeat/regression run before broadening
back to longer or higher-concurrency TCP acceptance.

## Progress

- Overall Knife14 estimate after this run: `91%`.
- Reason: clean reverse-first P1 is now outside the 10-20 Mbit/s band with
  stable-high attribution, and pending/terminal loss accounting stayed clean.
  Remaining work is repeat stability and broader regression, not another
  lifecycle root hunt.
