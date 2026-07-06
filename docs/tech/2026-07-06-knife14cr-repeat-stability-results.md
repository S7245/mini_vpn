# Knife14cr Repeat Stability Results

Date: 2026-07-06

## Code

- Code under test: Knife14cq default path from commit `d85f1ff`.
- No code changes were made in this stage.
- Remote source hash:
  `8578fa896716987a1f236af3c3455853e3954dce411963dd8166e6398cc84938`
- Remote binary hash:
  `040e2ad8760eee58c10d62386502a23d2c5a1471d5e6fcf8e8d57931346b617e`

## VPS Run

- Remote bundle:
  `/tmp/mini_vpn/knife14cr_repeat_stability_20260707_041152/mvpn_knife14cr_repeat_stability_usclient_suite_20260707_041152.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cr_repeat_stability_20260707_041152_local/`
- Suite shape: reverse-first P1 only, `P=1`, `30s`, MTU `1200`,
  server evidence disabled, explicit `.33` exit SSH variables set.

Direct baselines stayed healthy:

- `.27 -> .77`: `315/275 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `307/280 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `313/280 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `294/267 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 repeated stable high:

- `iperf_sender_mbps=174.000`
- `iperf_receiver_mbps=172.000`
- `overall_avg_mbps=172.406`
- `tail_avg_mbps=153.865`
- `tail_min_mbps=4.190`
- `tail_collapse=0`
- `throughput_shape=stable_high local_pressure=1 no_data=0`

The final one-second interval was low (`4.19 Mbit/s`), but the parser did not
classify the tail as collapsed because the six-sample tail remained high
overall.

## Clean Signals

- Startup again printed `TUN RX active-flow timer drain: 0ms`.
- Runtime diagnostics again kept `timer_active_flow_attempts=0`.
- TUN RX drain remained event-driven:
  `attempts=65834`, `pre_payload_attempts=32917`,
  `remote_payload_attempts=32917`, `maintenance_attempts=0`,
  `packets=177992`, `would_block=65752`, `errors=0`.
- Pending/terminal/reap stayed clean:
  `pending_at_close=0`, `egress_at_close=0`,
  `terminal_pending_reap=0`, `terminal_late_remote_payload=0`.
- Local drops and feedback stayed clean:
  `tun_tx_dropped_delta=0`, `drop_delta_total=0`,
  `pause_edges=0`, `resume_edges=0`.
- QUIC loss/congestion/blocking stayed zero.
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`.
- Current `.33` checks before and after the run showed no TUIC `fail auth`.
- `.27` cleanup succeeded after the suite.

## Interpretation

Knife14cr confirms Knife14cq was not a one-off. The default-disabled timer path
has now produced two clean reverse-first P1 high-throughput runs:

- Knife14cq: `182/181 Mbit/s`, `stable_high`.
- Knife14cr: `174/172 Mbit/s`, `stable_high`.

The remaining work is broader TCP regression: repeat beyond P1, higher
concurrency, and longer duration. The original Knife14 local
downlink/terminal-pending P1 root is now closed for clean reverse-first P1.

## Progress

- Overall Knife14 estimate after this repeat: `93%`.
- Reason: two consecutive default-path reverse-first P1 runs are stable high
  with clean pending/terminal/reap/drop/QUIC surfaces. Remaining risk is
  breadth and duration, not the prior 10-20 Mbit/s lifecycle collapse.
