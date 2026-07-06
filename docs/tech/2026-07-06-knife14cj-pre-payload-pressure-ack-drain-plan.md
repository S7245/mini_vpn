# Knife14cj Pre-Payload Pressure ACK Drain Plan

Date: 2026-07-06

## Design Tree

1. Raise the adaptive drain budget.
   - Rejected as the first move. Knife14ci showed timing is wrong:
     ACK drain happened after payload flush and all attempts exhausted.
     A bigger budget after the burst can still be late.
2. Drain TUN RX unconditionally on every timer tick.
   - Rejected. Knife14bi already rejected broad opportunistic drain.
3. Pressure-gated pre-payload and maintenance drain.
   - Strong. It keeps Knife14ci's pressure guard but moves ACK/window
     processing before additional downlink bytes are pushed into smoltcp/TUN
     and repeats while dirty pressure persists.
4. Revisit sing-box/TUIC auth.
   - Rejected for this stage. The first startup failure was transient; no-secret
     config/time checks matched, and retry reached P1.

## Implementation Plan

1. Add pure helpers:
   - pre-payload budget from ctx epoch, payload length, local socket snapshot,
     and pressure;
   - maintenance budget from dirty aggregate `DownlinkPressureStats`.
2. Extend `TunRxDrainDiag` with source counters:
   - pre-payload;
   - post-payload;
   - maintenance;
   - other.
3. In the remote payload branch, compute pre-payload budget before
   `handle_remote_payload`; if non-zero, call `drain_ready_tun_rx` with source
   `remote_payload_pre`.
4. Keep the existing post-payload drain as a secondary safety net.
5. In the timer branch, after `iface.poll + flush_tx` and before
   `process_dirty_relay`, compute dirty pressure and run maintenance drain with
   source `timer_pressure` if eligible.
6. Add focused tests.
7. Run local gates, commit, push.
8. Sync `.27`, run the scoped P1 suite, parse bundle, and record results.

## Test Plan

Focused:

- `cargo test tun_rx_drain --lib`
- `cargo test tun_rx_pressure --lib`
- `cargo test pre_payload --lib`
- `cargo test maintenance --lib`

Regression:

- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## Stop Conditions

- If source counters show pre/maintenance drain never engages, inspect whether
  dirty pressure is computed too late or the source socket is not in the dirty
  set.
- If pre/maintenance drain engages and drops vanish but throughput remains low,
  stop TUN RX timing work and inspect local TUN egress scheduling/flush cadence.
- If drops persist with budget exhaustion, consider adaptive multi-pass drain
  bounded by elapsed work, not a static env default.
