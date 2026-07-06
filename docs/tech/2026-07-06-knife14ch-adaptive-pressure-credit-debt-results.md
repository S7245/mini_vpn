# Knife14ch Adaptive Pressure Credit Debt Results

Date: 2026-07-06 / VPS local time 2026-07-07 01:42-01:50 CST

## Scope

Knife14ch changed pressure-triggered egress credit debt from a full-span freeze
to an adaptive debt:

- observed TUN drops still install the full bounded credit span;
- pressure edges install a guard-sized debt plus actual overshoot, capped at
  the original span.

The goal was to preserve Knife14cf's early warning without turning every
pressure edge into a sustained `10-20 Mbit/s` burst/stall run.

## Local Verification

Passed before VPS:

- `cargo test pressure_credit --lib`
- `cargo test credit_debt --lib`
- `cargo test drop_credit --lib`
- `cargo test downlink_egress_clock --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

Code/spec/plan commit: `0cc6d31`.

## VPS Runs

Common setup:

- Client: `.27` / `43.172.75.27`
- Exit: `.33` / `43.153.32.33`
- Target: `.77` / `43.130.32.77:5201`
- MTU: `1200`
- TCP pool: `2`
- `MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW` unset

### First Run

- Suite tag: `knife14ch_adaptive_pressure`
- Remote bundle:
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_20260707_0142/mvpn_knife14ch_adaptive_pressure_usclient_suite_20260707_014241.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_20260707_0142_local/`

The suite accidentally inherited `EXIT_TO_TARGET_IPERF_CHECK=0`, so its own
`.33 -> .77` baseline was skipped. A manual post-run `.33 -> .77` check was
healthy (`289/297 Mbit/s`), and current `.33` TUIC logs showed no `fail auth`.

The probe failed as `no_data`:

- Reverse-first P1 sender: `0.245 Mbit/s`
- Reverse-first P1 receiver: `0.020 Mbit/s`
- `pressure_credit_debt_bytes=0`
- `drop_credit_debt_bytes=0`
- `tun_tx_dropped_delta=0`
- `global_rx_pressure events=0`
- `pending_at_close=0`
- `egress_at_close` appeared only in the final close tail

Interpretation: this run did not exercise the adaptive pressure debt change.
The data stream read a small initial amount and then starved, so it is not a
valid rejection of the Knife14ch algorithm by itself.

### Repeat Run

- Suite tag: `knife14ch_adaptive_pressure_repeat`
- Remote bundle:
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_repeat_20260707_0149/mvpn_knife14ch_adaptive_pressure_repeat_usclient_suite_20260707_014902.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_repeat_20260707_0149_local/`

The repeat forced `EXIT_TO_TARGET_IPERF_CHECK=1` and proved both direct paths
were healthy:

- `.27 -> .77` forward receiver: `282 Mbit/s`
- `.27 <- .77` reverse receiver: `298 Mbit/s`
- `.33 -> .77` forward receiver: `266 Mbit/s`
- `.33 <- .77` reverse receiver: `297 Mbit/s`

The current `.33` window showed normal TUIC inbound/direct outbound entries for
`.27 -> .77`; there was no TUIC `fail auth`. The larger tail still contained
unrelated VLESS REALITY invalid-connection scanner noise.

## Repeat Result

Knife14ch repeat failed VPS acceptance.

- Reverse-first P1 sender: `16.0 Mbit/s`
- Reverse-first P1 receiver: `15.5 Mbit/s`
- Shape: `low_average`
- QUIC loss/congestion/blocking deltas: clean
- TUN drops: `0`
- TUN flush failures: `0`
- `send_slice_zero=0`, `send_slice_errors=0`

Key local evidence:

```text
downlink_flush: attempts=2828 send_queue_max=892928 headroom_limited=47 headroom_deferred_bytes=4143720 drain_credit_granted_bytes=47440710 drain_credit_planned_bytes=360398 drain_credit_used_bytes=360398 drop_credit_debt_bytes=0 pressure_credit_debt_bytes=0 hard_edge_guard_limited=47 hard_edge_guard_deferred_bytes=1076625 tun_flush_deferred=3
global_rx_pressure: events=0 queue_used_max=18 queue_capacity=1024
global_rx_receive: pause_edges=0 resume_edges=0
tun_drops: tun_tx_dropped_delta=0
runtime_tun_egress: drop_events=0 drop_delta_total=0
```

Close/lifecycle evidence:

```text
terminal_late_remote_payload: events=891 bytes=1834980
egress_at_close: events=1 bytes=2816 terminal_events=1
tcp_lifecycle: transitions=5 closed_edges=1 states=Established,SynReceived,Closed,CloseWait
relay_remote_timing: data_first_read_max_ms=3 data_max_read_gap_ms=4249 data_rx_bytes_max=60167114
tuic_tcp_stream: data_first_rx_max_ms=3 data_read_gap_max_ms=4249 data_rx_bytes_max=60167114 reads_max=5442
```

The decisive close line was:

```text
tcp-handle-close handle=SocketHandle(1) direction=local reason=dead_slot_reap ... pending=0 ... remote_to_global_rx_bytes=58332134 terminal_late_remote_payload_bytes=1834980 ... send_queue_max=892928 ... pressure_credit_debt_bytes=0 ... close_egress_class=terminal_closed_no_send close_egress_bytes=2816 tcp_state=Closed active=false can_send=false may_send=false send_queue=2816
```

## Interpretation

The first run showed an intermittent no-data/stream-starvation shape, but the
repeat restored the more actionable failure class: clean external path, clean
QUIC, no TUN drops, and low reverse throughput while local `send_queue` rides
the credit/hard-edge guard.

Knife14ch adaptive pressure debt was still not the hot-path limiter in the
repeat: `pressure_credit_debt_bytes=0`. That is because the current
pressure-debt code installs only on a downlink backpressure rising edge, while
the repeat touched the credit spend edge (`892928B`) without crossing the
pause edge (`917504B`).

However, adding more static debt at the guard edge is unlikely to be the next
best move. It would make counters cleaner by reading less remote data, but the
repeat's deeper signal is that local ACK/window processing is lagging while
remote data arrives. With explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`, the
remote-payload branch does not opportunistically drain TUN RX ACKs; it waits
for the main `wait_for_rx` select branch. Under sustained global RX activity,
that can keep smoltcp's send queue pinned near the hard edge.

## Decision

Do not continue pressure-debt variants as the primary Knife14 path.

The next stage should add a code-level, pressure-triggered TUN RX/ACK drain
that is off when queues are healthy and activates only near the egress credit
edge. This is distinct from enabling the fixed test knob
`MINI_VPN_TUN_RX_DRAIN_BUDGET`; it is an adaptive local-lifecycle repair that
should free smoltcp send capacity before the path relies on remote read
throttling or terminal close accounting.

## Progress

Overall Knife14 progress is `82%` after Knife14ch:

- positive: external path, sing-box auth, QUIC loss/blocking, TUN drops,
  stale pool, and receive-window growth are all rejected again for this result;
- positive: the repeat points to a concrete local code path: ACK/TUN RX drain
  lag while send_queue is near the credit guard;
- negative: clean reverse-first P1 remains in the `10-20 Mbit/s` class, and the
  close tail still records terminal late remote payload;
- next: implement adaptive pressure-triggered TUN RX drain and prove whether it
  lowers `send_queue_max`, `hard_edge_guard_limited`, and terminal late payload
  while moving P1 out of the low band.
