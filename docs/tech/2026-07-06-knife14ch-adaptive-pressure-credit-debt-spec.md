# Knife14ch Adaptive Pressure Credit Debt Spec

Date: 2026-07-06

## Stage Goal

Preserve Knife14cf's early pressure signal without forcing every pressure edge
to install a full drain-credit debt span. A pressure edge is an early warning,
not the same severity as an observed TUN drop.

## Background

Knife14cd restored reverse-first P1 to `180/179 Mbit/s` with pool=2, but left
TUN egress drops and final egress backlog. Knife14ce showed post-drop debt is
too late. Knife14cf moved debt earlier to the pressure edge and reduced TUN
drops, but throughput fell back to `20.5/19.5 Mbit/s`. Knife14cg rejected
solving this by increasing receive read-ahead: it inflated pending to
`2123091B` and increased TUN drops.

The remaining local-control problem is severity: a TUN drop is hard evidence
that credit spending was harmful; a pressure edge is only an early warning.
Treating both as a full-span credit freeze protects the edge but can turn the
data path into burst/stall.

## Non-Goals

- Do not change TUIC TCP pool size, sing-box config, iperf3, or QUIC
  congestion settings.
- Do not re-enable bounded global receive decoupling as a default.
- Do not tune TUN queue length or TUN RX drain budget.
- Do not add environment knobs for this stage.
- Do not hide pending, close, reap, or TUN-drop counters.

## Invariants

- Observed TUN drops still install the full bounded credit debt span.
- Pressure edges install debt only when they reach the existing credit edge.
- Pressure debt is derived from existing thresholds:
  - a minimum guard-sized nudge at the credit edge;
  - plus actual bytes above the credit edge;
  - capped by the original credit span.
- Clean headroom remains usable even while pressure debt is being paid.
- A drain observation that pays debt must not grant new extra credit in the
  same observation.
- Diagnostic fields continue using the existing `pressure_credit_*` counters
  and `tcp-egress-credit-debt installed_bytes=...` line.

## Acceptance

Local:

- Focused tests prove:
  - pressure edge debt is guard-sized rather than full-span;
  - pressure overshoot scales debt but caps at the full span;
  - TUN drop debt remains full-span;
  - paying pressure debt blocks stale credit without blocking clean headroom;
  - later clean drain can grant credit again.
- Existing pressure/drop credit tests remain green.
- Parser self-tests do not need field changes.
- Full Rust, harness, release, clippy, and whitespace gates pass before VPS.

VPS:

- Same scoped clean reverse-first P1:
  `.27` client, `.33` exit, `.77` target, default pool=2, MTU 1200,
  `PARALLEL_SET=1`, `DURATION=30`.
- Current-window `.33` sing-box evidence must still show no TUIC `fail auth`.
- Acceptance requires materially better reverse-first throughput than
  Knife14cf's `20.5/19.5 Mbit/s` while keeping TUN drops and final
  pending/egress backlog visible. Full Knife14 progress requires stable high
  throughput with final TUN drops and close backlog cleaned up.

## Progress Target

If adaptive pressure debt restores high throughput and improves drop/backlog
signals, Knife14 can move back toward `88-90%`. If it only restores throughput
while drops remain similar to Knife14cd, the stage is useful but not final. If
it remains in the `10-20 Mbit/s` band, the credit-debt line should be treated
as exhausted and the next step is local egress architecture review rather than
another pressure-debt variant.
