# 2026-07-09 Knife14gs Local Egress Service G5 Results

## Goal

Implement the next architecture slice required by the Knife14gr performance
gate and stop at the local G5 capacity gate before VPS acceptance.

Non-goals stayed unchanged: no VPS retune, no iperf3 changes, no MTU/PLPMTUD
work, no stale-pool work, no broad QUIC window work, and no more read-credit
floor or self-wake work as the main branch.

## Code

Main change: `src/client_tun.rs` now has a bounded local egress service lane in
the main loop.

The lane runs after useful remote payload work and after timer dirty
maintenance. Within one active window it can repeat:

```text
TUN RX ACK intake
-> iface.poll
-> flush_tx
-> process_dirty_relay / flush_downlink
-> stop on target, no-progress, hard-pause, no-work, or cycle budget
```

New diagnostics:

- `tcp-local-egress-service windows=... cycles=... accepted_bytes=...`
- stop reason counters:
  `target_reached`, `no_progress`, `cycle_budget`, `hard_pause`, `no_work`
- `tcp-tun-rx-drain` now separates `local_egress_service_attempts`

The implementation preserves the existing hard guards:

- per-flow/global buffered pending guards;
- TUN drop feedback and active credit debt hard pause;
- terminal no-send/close-tail guards;
- bounded cycle and TUN RX packet budgets.

## G5 Capacity Gate

The code-level G5 floor is:

- `LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW = 128KiB`
- active window assumption: `5ms`
- nominal local service capacity: about `25.6 MB/s`, or about `204 Mbit/s`

This exceeds the `100 Mbit/s` path requirement of about `12.5 MB/s` plus
overhead, so the local code-level capacity gate is satisfied. This is not a VPS
throughput claim; it only means the next VPS run is worth doing.

## TDD And Local Gates

Focused tests added:

- local egress diagnostics report cycles, accepted bytes, TUN RX packets, and
  stop reasons;
- default capacity floor covers the `100M` path in code-level math;
- service lane stops on no-work, hard-pause, and no-progress instead of
  spinning.

Local gates passed:

- `cargo test local_egress_service -- --nocapture`
- `cargo test --lib`
- `cargo test`
- `cargo test --features harness --test concurrency_harness -- --nocapture`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `git diff --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo build --release`

Harness note: the `loop_profiler_detects_on_loop_cpu_saturation` test needed a
test-only assertion update. The new egress lane makes the no-burn baseline more
active, so `loop-active +0.05` is no longer a stable expectation. The stable
signal for the synthetic burn remains `poll_fraction`, which rose strongly in
the passing harness run.

## Interpretation

Knife14gs is a real architecture change in the local egress scheduler. It is
not a VPS result and does not prove `>30 Mbit/s` or `100+ Mbit/s` by itself.

It closes the specific code-level gap identified by Knife14gr: the B7-era path
had enough nominal flush capacity, but no single bounded service lane that
continued local ACK/TUN/smoltcp progress inside an active reverse-flow window.

## Next Gate

Run the same focused G6 VPS acceptance shape:

- safe1200 reverse-first P1
- `DURATION=30`
- `PARALLEL_SET=1`
- `MINI_VPN_BUFFERED_DOWNLINK=1`
- receiver must exceed `30 Mbit/s`

G6 must also inspect the new counters. A useful pass should show local egress
service cycles and accepted bytes without a dominant no-progress shape, while
TUN drops, send errors, close-tail accounting, and QUIC blocking remain clean.

If the receiver still stays below `30 Mbit/s`, compare
`tcp-local-egress-service`, `tcp-tun-rx-drain`, `tcp-downlink-flush`, TUN
drops, and QUIC blocking before modifying code again.
