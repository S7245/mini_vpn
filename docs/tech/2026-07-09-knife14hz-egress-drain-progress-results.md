# 2026-07-09 Knife14hz Egress Drain Progress Results

## Goal

Run the three-step H gate after Knife14gy:

- H1: define the local admission/headroom progress contract and add red tests;
- H2: implement local egress-progress feedback for TUN TX drain and service
  diagnostics;
- H3: run the first VPS threshold gate and stop.

This stage deliberately did not tune VPS services, iperf3, MTU/PLPMTUD, stale
TUIC pool handling, or broad QUIC windows.

## Code State

Code commit:

- `8fc0cdb` (`fix(knife14): count local egress drain progress`)

Main code changes:

- Added `TunIo::queued_tx_bytes()` so the local egress service can observe
  TUN TX queue drain instead of only new `accept_slice` bytes.
- Added `egress_drain_bytes` to `LocalEgressServiceProgress`,
  `LocalEgressServiceDiag`, and stream-service window diagnostics.
- Counted successful TUN TX queue drain or send-queue drain as useful local
  progress, while keeping failed flushes and empty flushes from faking
  progress.
- Kept the local egress service bounded by the existing cycle limits.

Focused TDD:

- `local_egress_service_progress_counts_tun_tx_drain_as_progress`
- `local_egress_service_diag_reports_capacity_progress_and_stop_reason`
- stream-service window tests covering `local_egress_drain_bytes` as useful
  progress.

Local gates passed:

- `cargo test local_egress_service --lib -- --nocapture`
- `cargo test stream_service --lib -- --nocapture`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `cargo fmt --check`
- `git diff --check`

Remote `.27` focused gates passed in a clean worktree at
`/tmp/mini_vpn_knife14_h3_8fc0cdb`:

- `cargo test local_egress_service --lib -- --nocapture`
- `cargo test stream_service --lib -- --nocapture`
- `cargo build --release`

## VPS Acceptance

Suite:

- tag: `knife14hz_egress_drain_p1`
- remote bundle:
  `/tmp/conn/mvpn_knife14hz_egress_drain_p1_usclient_suite_20260709_132051.tar.gz`
- local bundle:
  `/tmp/mini_vpn_knife14hz_egress_drain/mvpn_knife14hz_egress_drain_p1_usclient_suite_20260709_132051.tar.gz`
- report:
  `/tmp/conn/mvpn_knife14hz_egress_drain_p1_usclient_suite_20260709_132051.md`
- tunnel report:
  `/tmp/conn/mvpn_knife14hz_egress_drain_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_132051.md`

Preflight:

- `.33` sing-box active.
- `.77` iperf3 active.
- `.33` socket buffers remained high:
  `rmem_max=16777216`, `wmem_max=16777216`,
  `rmem_default=1048576`, `wmem_default=1048576`.
- Direct baseline remained healthy:
  `.27 -> .77` receiver `279 Mbit/s`,
  `.77 -> .27` receiver `277 Mbit/s`.

Reverse-first P1 result:

| Metric | Value |
| --- | ---: |
| iperf sender | `15.300 Mbit/s` |
| iperf receiver | `14.300 Mbit/s` |

The first threshold failed. The run did not exceed `30 Mbit/s` and did not
approach the final `100+ Mbit/s` target.

The iperf shape was bursty with many zero-throughput seconds. Examples:

- `0-1s`: `41.9 Mbit/s`
- `1-5s`: `0`
- `5-6s`: `105 Mbit/s`
- later windows alternated between short bursts and `0` buckets.

## Key Signals

Clean or rejected surfaces:

- `tun_rx_dropped_delta=0`
- `tun_tx_dropped_delta=0`
- `global_rx_pressure=0`
- `global_rx_queue_used_max=66/1024`
- `local_write_pressure=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `tun_flush_deferred=0`
- `pending_at_close=0`
- `terminal_pending_reap=0`
- QUIC loss/congestion/blocking deltas were `0`

The local pressure/headroom surface was also clean in this run:

- `may_recv_false=0` in the summary window
- `budget_limited=0`
- `headroom_limited=0`
- `pressure_credit_debt_bytes=0`
- `hard_edge_guard_limited=0`
- `send_queue_max=325095`
- `downlink_flush accepted_bytes=52039183`
- `remote_to_global_rx_bytes=52039183`

The new H2 local egress-progress diagnostic did not show useful drain
progress:

- `tcp-local-egress-service windows=3497`
- `cycles=323`
- `accepted_bytes=0`
- `egress_drain_bytes=0`
- `tun_rx_packets=99`
- `flush_tx_calls=323`
- `flush_tx_failures=0`
- `no_progress=269`
- `no_work=3228`

This means the H2 contract is valid instrumentation, but it was not the missing
throughput lever in the VPS run.

The remaining bad shape is ordered TUIC stream read cadence:

- data stream `remote_read_service_len_min=65536`
- data stream `remote_read_service_len_max=65536`
- data stream `remote_batches=3485`
- data stream `remote_batch_bytes_max=131072`
- data stream `read_credit_pause_updates=0`
- data stream `read_credit_limit_bytes_min=65536`
- data stream `max_remote_read_gap_ms=5086`
- data stream `data_read_gap_max_ms=5086`
- data stream `data_pending_gap_max_ms=5086`
- `connection_fresh_stream_frames_pending=8`
- `connection_stale_stream_frames_pending=11`
- `connection_rx_no_stream_frames=3`

Attribution:

- `terminal_late_remote_payload`
- `terminal_closed_late_payload`
- `egress_at_close`
- `tuic_stream_read_gap`
- `tuic_stream_read_pending`
- `relay_remote_read_gap`

Close-tail was mostly clean, but one terminal late payload remained:

- `terminal_late_remote_payload=1408 bytes`
- `egress_at_close=1408 bytes`
- `close_egress_class=terminal_closed_no_send`

## Conclusion

Knife14hz completed H1/H2/H3, but H3 failed the first throughput threshold.
The result was `15.3/14.3 Mbit/s`, so the answer to whether this exceeded
`30 Mbit/s` is no.

This stage disproves the specific hypothesis that missing TUN TX drain progress
inside `service_local_egress_until` was the active root. The H2 code is still
useful because it prevents future diagnostics from treating TUN TX drain as
invisible, but it did not recover throughput.

The next code design must stop local egress-drain and pressure-credit tuning
for this branch. The active root is the ordered TUIC stream read/pending
self-wake cadence: during active reverse traffic, the relay can see fresh or
stale stream-frame pending evidence while useful reads still stall for multiple
seconds. The next acceptance gate should require no active data-read gap above
`500ms`, near-zero fresh pending events, and throughput above the `30 Mbit/s`
first threshold before continuing toward `100+ Mbit/s`.
