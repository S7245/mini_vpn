# Knife14bv Close Egress Drain Plan

## Plan

1. Observability and cleanup slice.
   - Remove or test-gate the unused `observe_pressure` wrapper so release builds
     are warning-clean.
   - Add raw/effective pressure fields to the downlink backpressure transition
     line.
   - Add close-time egress queue classification beside existing pending
     accounting.
   - Add focused tests for those diagnostics.
   - Run focused tests, `cargo test --lib`, release build, and `git diff
     --check`.
   - Commit/push as a behavior-neutral task.

2. Bounded close-egress drain slice.
   - Add a small per-flow deferred close state for relay close with queued
     smoltcp egress.
   - TDD the predicate: defer only when the socket is active, send-capable, and
     its send queue is above the resume watermark.
   - Finish the deferred close once send queue drains to/below low.
   - Bound the wait with a short grace if the queue does not drain.
   - Keep existing app-pending deferred close behavior unchanged.
   - Commit/push as a behavior task.

3. Acceptance slice.
   - Sync `.27` to the behavior commit.
   - Preflight `.33` sing-box/TUIC and `.77` iperf3.
   - Run one scoped reverse-first P1 with server evidence and stop after the
     clean window.
   - Parse bundle, compare against Knife14bu, update results and learning.

## Risk Review

- Risk: delaying rearm after remote EOF leaves a slot occupied longer.
  - Mitigation: defer only when queued egress is observable and bound the grace.

- Risk: queued smoltcp bytes might already be harmless after iperf app close.
  - Mitigation: record close-egress classification first; behavior tests target
    lifecycle correctness, and VPS acceptance decides throughput value.

- Risk: raw/effective pressure log fields could confuse existing parser output.
  - Mitigation: append fields without removing existing keys.

- Risk: release warning cleanup accidentally removes test coverage.
  - Mitigation: update tests to call timestamped methods directly.

