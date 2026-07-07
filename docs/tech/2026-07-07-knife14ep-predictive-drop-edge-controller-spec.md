# Knife14ep Predictive Drop-edge Controller Spec

Date: 2026-07-07

## Stage Goal

Close the remaining Knife14 local downlink control gap exposed by the
Knife14eo 97% VPS run: the controller reduced headroom debt but still allowed
projected remote payload credit to push local TUN egress pressure over the drop
edge before feedback became strong enough.

## Evidence

Knife14eo scoped safe1200 reverse-first P1:

- `32.3/30.8 Mbit/s`, below the `100+ Mbit/s` target.
- `tun_tx_dropped_delta=117`.
- `send_queue_max=553848`, `pending_high=576836`, `pending_total=97919`.
- `may_recv_false=2129` and `headroom_deferred_bytes=50881438`, improved from
  Knife14en but not clean enough.
- QUIC loss/congestion/blocking remained zero.
- close/reap counters remained clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`.

## Non-goals

- Do not tune sing-box, iperf3, stale pool, connection pool, QUIC MTU, or
  PLPMTUD.
- Do not raise TUN queue length or revive the rejected blunt egress pacer path.
- Do not hard-pause relay reads as the normal pressure response; preserve a
  small ACK/window drain path unless bounded staging is full or TUN feedback is
  paused.

## Required Behavior

- Projected relay read credit must shrink before a remote payload can push
  `send_queue + pending` through the local egress edge.
- Under fresh pressure/drop debt, controller read credit and flush budget may
  shrink below the old pressure floor; the old `16 * MTU` floor was still too
  large near the drop edge.
- A small non-paused ACK/window drain batch remains available while bounded
  staging has room.
- Bounded staging remains the hard local pending limit.
- Observed egress progress still grows credit additively.

## Acceptance

Local:

- Focused tests for projected drop-edge credit and controller sub-floor shrink.
- Existing relay-read, pressure-credit, deferred ACK, and relay-ready burst
  tests remain green.
- Full lib/harness/clippy/script/release gates remain green.

VPS:

- `.27 -> .33 -> .77`, safe1200, reverse-first P1.
- receiver `100+ Mbit/s`.
- `tun_tx_dropped_delta=0`.
- `may_recv_false` and `headroom_deferred_bytes` stay far below Knife14en.
- `pending_at_close=0`, `terminal_pending_reap=0`, `egress_at_close=0`.
- QUIC loss/congestion/blocking remain zero.
