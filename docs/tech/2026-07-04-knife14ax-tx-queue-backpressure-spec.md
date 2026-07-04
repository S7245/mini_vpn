# Knife14ax spec - tx-queue-aware downlink backpressure

Date: 2026-07-04

## Grounding

Knife14aw added send-window and relay queue diagnostics. The clean
reverse-first P1 acceptance still failed at `16.4/15.0 Mbit/s`, while direct
baselines on both `.27 <-> .77` and `.33 <-> .77` were about `196-198 Mbit/s`.

The run showed `send_capacity_min=max=1048576`, `send_queue_max=1048576`,
`pending_total=0`, `global_rx_queue_used_max=258/1024`, clean-window QUIC
loss/congestion zero, `send_slice` zero/error zero, and high TUN egress drops.

## Problem

The current downlink backpressure loop only considers app-owned
`SocketCtx.downlink_pending`:

```text
global_rx_paused = next_downlink_backpressure(..., downlink_pending_stats(...))
```

After `send_slice` accepts bytes into smoltcp, `downlink_pending` can be empty
even though smoltcp's tx queue is full and the local TUN egress path is the
actual limiter. In that state `global_rx` resumes remote reads too early.

## Goal

Make downlink backpressure account for smoltcp tx-queue pressure without
changing unrelated transport behavior.

## Non-Goals

- Do not tune TUN queue length.
- Do not change egress pacing defaults.
- Do not change close/reap predicates or grace windows.
- Do not change TUIC TCP pool behavior.
- Do not change iperf3, sing-box, or VPS service configuration.
- Do not store secrets or credential-bearing logs in docs or learning memory.

## Invariants

- Existing app-owned `downlink_pending` high/low behavior is preserved.
- A socket with `downlink_pending=0` but high smoltcp `send_queue` can pause
  relay `global_rx`.
- While paused, `global_rx` resumes only after both app-owned pending and
  smoltcp tx queue pressure are at or below the low watermark.
- Parser compatibility is preserved for older logs.
- Existing diagnostic fields keep their names.

## Acceptance

Local:

- focused Rust tests prove tx queue pressure triggers high/low hysteresis even
  when app-owned pending is zero;
- existing downlink pending/backpressure tests still pass;
- low-RTT probe self-test covers the new summary fields;
- US-client suite self-test still passes;
- `cargo test --lib client_tun`;
- `cargo test`;
- `cargo test --features harness`;
- `cargo clippy --all-targets --features harness -- -D warnings`;
- `git diff --check`.

VPS:

- scoped `.27` reverse-first P1 run from the pushed commit;
- report includes tx-queue pressure fields next to existing downlink
  backpressure and flush fields;
- clean reverse-first P1 should leave the `10-20 Mbit/s` band or show a new
  explicit limiter rather than hiding behind `pending_total=0`.
