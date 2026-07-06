# Knife14cb Egress-Progress-Clocked Accept Spec

Date: 2026-07-06

## Grounding

Knife14ca proved the fixed pre-`send_slice` headroom cap worked mechanically:
`send_queue_max` stayed at `720896B`. It still regressed reverse-first P1 to
`16.1/14.8 Mbit/s` and produced final evidence of receive-window starvation:
`pending=566509`, `may_recv_false=11897`, and
`headroom_deferred_bytes=3317103134`.

The rejected shape is a static queue-occupancy cap. When `send_queue` reaches
the clean flush threshold, the current limiter can return `0` even while the
local side is making real egress progress between observations.

## Stage Goal

Replace threshold-only downlink acceptance with an egress-progress-clocked
limit:

- keep the clean queue headroom guard;
- grant bounded extra accept credit only when the observed smoltcp send queue
  has decreased since the previous downlink observation;
- consume that credit when accepting above the clean threshold; and
- never let one flush pass plan beyond the existing hard tx_queue pause
  threshold.

## Non-goals

- Do not tune stale pool, iperf3, sing-box, QUIC, TUN txqueuelen, or external
  pacer settings.
- Do not remove pending safety, close/reap accounting, terminal late-payload
  accounting, or TUN egress feedback.
- Do not re-open unbounded remote-read bursts.

## Invariants

- Bytes that cannot be accepted remain in `downlink_pending`.
- No observed drain means no drain credit.
- Drain credit is capped by the space between the clean flush threshold and the
  hard pause threshold.
- Planned `send_slice` length is bounded by pending length, configured
  per-flush budget, smoltcp send capacity, clean headroom plus credit, and hard
  pause headroom.
- Credit is consumed only for bytes accepted above clean headroom.
- Diagnostics must expose granted and used drain credit so VPS evidence can
  distinguish progress-clocked accepts from hidden loss.

## Acceptance

Local:

- RED/GREEN tests for no-credit behavior and observed-drain credit behavior.
- Existing pending, close/reap, backpressure, pacer, TUN feedback, parser, and
  harness tests stay green.
- `cargo test --lib`, script self-tests/syntax checks, release build, harness
  tests, clippy with harness, and `git diff --check` pass before VPS.

VPS:

- Re-run the same `.27 -> .33 -> .77` reverse-first P1 suite with final
  lifecycle summary enabled.
- Require receiver throughput to climb back above Knife14by's `29.0 Mbit/s`
  reference or produce a clearly better stop/go shape without hiding final
  pending/drop evidence.
- Require final parser summary to include post-probe close/drop accounting.
