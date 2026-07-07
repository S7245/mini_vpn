# Knife14eo Local Downlink Credit Controller Spec

Date: 2026-07-07

## Goal

Replace the remaining loosely coupled downlink pressure heuristics with a
per-flow local downlink credit controller. Relay read credit, flush budget, and
headroom debt must be coupled to observed smoltcp/TUN egress progress instead
of only reacting to estimated queue pressure after bytes are already accepted.

## Evidence

Knife14en safe1200 rejected QUIC MTU/PLPMTUD as the primary root. PLPMTUD was
disabled, QUIC stream frames still arrived, and loss/congestion/blocking stayed
zero, but reverse-first P1 remained `24.3/23.1 Mbit/s`.

The live failure signal moved back to local downlink pressure:

- `downlink_backpressure pause_edges=4 resume_edges=3`
- `max_pending=725572`
- `send_queue_max=556664`
- `may_recv_false=14700`
- `headroom_limited=15691`
- `headroom_deferred_bytes=4018847472`
- `pressure_credit_blocked_bytes=1907512`

The clean surfaces are still important: terminal pending reap, pending at
close, egress at close, TUN drops, send-slice errors, TUN flush failures, QUIC
loss, QUIC congestion, and QUIC flow-control blocking were all zero.

## Design Tree

- Reject server/path/iperf/sing-box/stale-pool/MTU work unless new evidence
  contradicts Knife14en.
- Reject hard relay-read pauses driven by local pressure debt; Knife14dz showed
  that this can create QUIC receive-window blocking.
- Keep relay reads draining into bounded local staging, but make the staging
  size and batch size shrink quickly when local egress stops progressing.
- Treat `headroom_deferred_bytes` as hard feedback, not passive accounting:
  deferrals reduce both future relay read batch size and the next flush budget.
- Let observed send-queue decreases grow credit additively. If no egress
  progress is observed across repeated headroom-limited flushes, retain only a
  small bounded staging allowance while ACK/window drains continue.

## Invariants

- A single flow's controller must not affect unrelated relay sessions.
- The controller must not increase pending beyond a bounded staging target when
  egress is stalled.
- TUN feedback pause and receive-window high-water remain hard stops.
- Progress recovery must be additive and bounded; pressure shrink must be fast.
- Existing lifecycle accounting remains visible: pending-at-close and terminal
  pending reap must stay zero in acceptance.

## Acceptance

- Local deterministic tests cover shrink, bounded staging, progress recovery,
  and flush-budget compression.
- Local gates pass: focused controller/read-credit/downlink tests, full
  `cargo test --lib`, script self-tests, release build, and `git diff --check`.
- `.27` focused gates pass after sourcing `.cargo/env`.
- VPS reverse-first P1 on `.27 -> .33 -> .77` reaches receiver `100+ Mbit/s`.
- Parsed signals improve: `may_recv_false` and `headroom_deferred_bytes`
  significantly decrease, pending-at-close and terminal-pending-reap remain
  zero, TUN drops remain zero, and QUIC loss/blocking remains zero.
