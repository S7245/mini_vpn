# Knife14at spec - runtime TUN egress feedback

Date: 2026-07-04

## Grounding

Knife14as tested commit `2205734` on `.27` and failed throughput acceptance, but
it closed the terminal-pending lifecycle question:

- clean reverse-first P1 reached only `22.600/22.200 Mbit/s`;
- `terminal_pending_reap=0` during the measurement window;
- the terminal close tail appeared only after the local application closed;
- clean-window QUIC loss/congestion deltas were zero;
- `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`, and
  `tun_flush_deferred=0`;
- active attribution was `local_tun_egress_drop+local_downlink_backpressure`;
- `tun_tx_dropped_delta=703`.

The remaining active branch is local egress capacity: bytes reach mini_vpn from
the remote stream and are accepted into smoltcp/TUN flush paths, but the Linux
TUN egress side still drops packets while downlink backpressure pauses remote
reads.

## Problem

The current suite can sample `ip -s link` before and after a probe, but the
mini_vpn process log itself does not report kernel TUN egress drop deltas. That
leaves a timing gap:

- a probe-level `tun_tx_dropped_delta` proves drops happened sometime during the
  iperf window;
- `tcp-downlink-flush` proves mini_vpn accepted/flushed bytes over time;
- `tcp-downlink-backpressure` proves remote reads were paused over time;
- but no single process-local line correlates TUN egress drops with the same
  downlink/backpressure diagnostic sample.

Connecting Linux `tx_dropped` directly to product behavior would also be too
narrow for the future cross-platform product direction. The first safe step is
to make the runtime feedback observable and testable without changing data-plane
behavior.

## Goal

Add a behavior-neutral runtime TUN egress diagnostic:

- remember the OS TUN interface name after device creation when the platform
  exposes it;
- while TCP diagnostics are enabled, periodically read Linux
  `/sys/class/net/<if>/statistics/tx_dropped`;
- log `tcp-tun-egress` with total and delta drop counters alongside current
  downlink pending/flush/backpressure state;
- keep non-Linux and unavailable-stat paths as explicit no-op/unknown states;
- extend the low-RTT parser and suite grep so VPS reports show the runtime
  sampler summary.

## Non-Goals

- Do not use Linux `tx_dropped` as a product control input in this slice.
- Do not change downlink egress pacing, flush budget, backpressure watermarks,
  TUN queue length, TUIC pool, QUIC congestion control, or iperf settings.
- Do not make sysfs sampling mandatory for macOS, iOS, Android, or Windows.
- Do not hide local egress drops by increasing queue length.
- Do not store secrets or raw environment output in docs or learning memory.

## Design Tree

1. Make Linux `tx_dropped` drive runtime backpressure immediately.
   Rejected for this slice. It would be Linux-specific product behavior and
   needs a clearer controller contract before it can be cross-platform safe.

2. Add sleeps between TUN writes.
   Rejected. Sleeping in the main loop risks unrelated flow stalls and repeats
   the kind of blunt pacing Knife14aq rejected.

3. Tune downlink watermarks or TUN queue length.
   Rejected for this slice. Knife14as says the remaining branch is egress
   feedback, not another blind knob.

4. Add process-local TUN egress telemetry first.
   Selected. It closes the timing gap, is deterministic enough to unit test via
   injectable reads, and leaves behavior unchanged.

## Invariants

- Default data-plane behavior is unchanged when diagnostics are off.
- Missing interface name, missing sysfs, parse errors, and counter wrap do not
  affect relay behavior.
- Counter deltas are monotonic only when the kernel counter is monotonic; lower
  samples produce `unknown` deltas rather than underflow.
- `tcp-tun-egress` lines include enough downlink context to correlate drops with
  `tcp-downlink-flush` and `tcp-downlink-backpressure`.
- The low-RTT parser remains backward compatible with logs that do not include
  `tcp-tun-egress`.

## Acceptance

Local:

- unit tests cover stat parsing, delta calculation, no-op states, and formatting;
- low-RTT probe self-test covers `tcp-tun-egress`;
- `cargo test --lib client_tun`;
- `bash -n scripts/knife14b-lowrtt-probe.sh`;
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`;
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`;
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`;
- `git diff --check`.

VPS:

- run one scoped reverse-first suite from `.27`;
- report includes `runtime_tun_egress`;
- if low throughput remains, compare runtime TUN drop deltas with
  downlink/backpressure samples to decide whether the next stage should add a
  portable egress controller or re-evaluate the architecture again.
