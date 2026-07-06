# Knife14cg Bounded Global RX Receive Window Results

Date: 2026-07-06

## Scope

Knife14cg tested whether TUIC/global receive progress should be decoupled from
local TUN egress pressure behind a bounded app-owned receive window.

The first implementation made the split active for the VPS A/B run. That run
rejected the idea as a product default, so the code was changed after the run
to keep the behavior as an explicit diagnostic A/B only:

- default: legacy safe gate, `downlink_rx_paused || tun_egress_feedback`
- opt-in A/B: `MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1`

This preserves the parser and diagnostics without shipping the failed behavior
by default.

## Local Verification

Passed:

- `cargo test global_rx_receive --lib`
- `cargo test tun_runtime_config_defaults_match_stage9_behavior --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Run

- Client: `.27` / `43.172.75.27`
- Exit: `.33` / `43.153.32.33`
- Target: `.77` / `43.130.32.77:5201`
- Suite tag: `knife14cg_global_rx_receive`
- Bundle:
  `/tmp/mini_vpn/knife14cg_global_rx_receive_20260707_0126/mvpn_knife14cg_global_rx_receive_usclient_suite_20260707_012632.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cg_global_rx_receive_20260707_0126_local/`

The run used a synchronized `.27` working tree with the Knife14cg receive split
active. The post-run code now gates that split behind
`MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW`.

## Preflight

Direct baselines were healthy:

- `.27 -> .77` forward receiver: `282 Mbit/s`
- `.27 <- .77` reverse receiver: `277 Mbit/s`
- `.33 -> .77` forward receiver: `281 Mbit/s`
- `.33 <- .77` reverse receiver: `297 Mbit/s`

The current-window `.33` sing-box log showed normal TUIC inbound and direct
outbound entries for `.27 -> .77`. There was no current-window TUIC `fail auth`.
Unrelated VLESS REALITY invalid-connection lines from other source IPs were
present in the larger log tail and are not evidence against this TUIC run.

## Result

Knife14cg failed VPS acceptance.

- Reverse-first P1 sender: `20.0 Mbit/s`
- Reverse-first P1 receiver: `18.7 Mbit/s`
- Shape: `low_average`
- QUIC loss/congestion/blocking deltas: clean
- `send_slice_zero=0`, `send_slice_errors=0`
- `tun_flush_failures=0`

The new receive-window signal engaged:

```text
global_rx_receive: pause_edges=1 resume_edges=0 max_pending_bytes=2123091 max_total_pending_bytes=2123091 max_tx_queue_bytes=892928 receive_high=2097152 receive_low=524288 receive_total_high=4194304 receive_total_low=1048576 local_egress_paused=1 tun_feedback_paused=0
```

But it engaged only after the run had already accumulated harmful local backlog:

```text
downlink_flush: attempts=14952 ... send_queue_max=892928 ... may_recv_false=6841 ... headroom_limited=8032 ... headroom_deferred_bytes=2067459595 ... pending_total_max=2123091
tcp_reverse_window: events=86 payload_bytes=3174623 accepted_bytes=1051532 pending_max=2123091 ... may_recv_false=78
tun_drops: tun_tx_dropped_delta=4051
tun_egress_feedback: pause_edges=1 resume_edges=0 drop_events=1 drop_delta_total=3511 max_delta=3511 max_pressure_bytes=2123091
```

Final suite lifecycle confirmed the same shape:

```text
final_pending_at_close: events=1 bytes=2123091 max_bytes=2123091 send_capable_events=1 send_capable_bytes=2123091
final_egress_at_close: events=1 bytes=892928 max_bytes=892928 send_capable_events=1 send_capable_bytes=892928 drain_candidate_events=1 drain_candidate_bytes=892928
final_global_rx_receive: pause_edges=1 resume_edges=1 max_pending_bytes=2123091 max_total_pending_bytes=2123091 receive_high=2097152 receive_low=524288
final_runtime_tun_egress: samples=43 drop_events=2 drop_delta_total=4051 max_delta=3511
final_tun_egress_feedback: pause_edges=2 resume_edges=1 drop_events=2 drop_delta_total=4051 max_delta=3511 max_pressure_bytes=2123091
```

## Interpretation

The bounded receive split did exactly what it was designed to do: it allowed
TUIC/global receive to continue until app-owned pending reached the receive
window. The VPS result shows that this is the wrong default behavior for the
current bottleneck. It moves bytes from the remote stream into local pending,
but the local TUN/smoltcp egress path still cannot drain them cleanly. The
result is larger pending, more TUN drops, and the same low reverse throughput.

This rejects the branch that "remote receive is being paused too early by local
egress feedback." The active root is still local egress consumption/cadence:
how quickly and smoothly smoltcp/TUN turns accepted downlink bytes into packets
without filling the local queue and without hiding close-time backlog.

## Code Decision

The receive split remains useful as a diagnostic A/B and parser signal, but not
as a default. Product/default behavior stays on the prior safe receive gate
unless `MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1` is set explicitly.

## Progress

Overall Knife14 progress is tracked as `84%` after this stage:

- positive: the receive-window hypothesis is now tested and rejected with
  concrete counters, and the failed behavior is gated off by default;
- negative: clean reverse-first P1 is still in the `10-20 Mbit/s` band and the
  run exposed larger send-capable close backlog;
- next: stop increasing receive windows. The next coherent patch should target
  local egress drain/cadence directly, with tests that prove useful queued
  bytes are flushed by local progress rather than merely buffered in pending.
