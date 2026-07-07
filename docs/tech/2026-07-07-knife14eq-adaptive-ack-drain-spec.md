# Knife14eq Adaptive ACK Drain Spec

Date: 2026-07-07

## Stage Goal

Fix the Knife14ep over-throttle failure without reopening the TUN drop edge.
Knife14ep proved that a tiny fixed ACK/window drain floor removes TUN drops, but
it also starves reverse TCP receive cadence. Knife14eq keeps the predictive
drop-edge cap and makes the pressure-edge ACK/window drain floor adaptive from
observed clean local egress progress.

## Evidence

Knife14ep scoped safe1200 reverse-first P1:

- Tunnel result: `30.3/28.7 Mbit/s`, below the `100+ Mbit/s` target.
- Clean surfaces:
  `tun_tx_dropped_delta=0`, QUIC loss/congestion/blocking `0`,
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`.
- Remaining local cadence pressure:
  `may_recv_false=8335`, `headroom_deferred_bytes=20226670`,
  `send_queue_max=557240`, data stream read/pending gaps around `3.47s`.
- The sub-floor path was active: data-stream `read_credit_limit_bytes_min`
  reached about `1111` bytes.

## Non-goals

- Do not revisit stale pool, iperf3, sing-box, QUIC MTU, PLPMTUD, connection
  pool, TUN queue length, or the rejected blunt egress pacer path.
- Do not make relay reads hard-pause under ordinary local pressure. Hard pause
  remains for full bounded staging or explicit TUN feedback pause.
- Do not simply restore the old static `16 * MTU` pressure floor everywhere.

## Required Behavior

- At the flush edge with no recent egress progress, keep the tiny ACK/window
  drain floor from Knife14ep.
- When observed local egress progress is clean, grow the non-paused pressure
  drain floor to a bounded multi-MTU batch.
- Near the credit/high-water edge, clamp back to the tiny floor before local
  pending expands.
- Consecutive no-progress/deferred pressure shrinks the adaptive floor quickly.
- Bounded staging remains the hard pending limit.
- Close/reap accounting and QUIC receive-window progress stay clean.

## Acceptance

Local:

- Focused TDD for clean-progress adaptive ACK/window drain growth.
- Focused TDD for high-water clamp and no-progress shrink.
- Existing relay-read, projected-pressure, downlink-credit, pressure-credit,
  deferred ACK, and relay-ready burst tests remain green.
- Full lib/harness/clippy/script/release gates remain green before acceptance.

VPS:

- `.27 -> .33 -> .77`, safe1200, reverse-first P1.
- Receiver `100+ Mbit/s`.
- `tun_tx_dropped_delta=0`.
- `may_recv_false` and `headroom_deferred_bytes` significantly below
  Knife14en, and ideally below Knife14ep.
- `pending_at_close=0`, `terminal_pending_reap=0`, `egress_at_close=0`.
- QUIC loss/congestion/blocking remain zero.
