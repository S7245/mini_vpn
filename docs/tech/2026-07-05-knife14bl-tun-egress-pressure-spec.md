# Knife14bl TUN Egress Pressure Spec

Date: 2026-07-05

## Stage Goal

Make TCP reverse/downlink handling stop forcing immediate TUN flushes when the
local smoltcp send queue is already at the downlink backpressure high watermark.

Knife14bk proved the target iperf3 sender was also limited to the low tunnel
rate, while QUIC loss/congestion, sing-box auth/time/config, target iperf3,
terminal pending reap, and hidden server-side bulk-send loss were not the root.
The remaining useful branch is local TUN egress pressure feeding back into the
TCP receive window and lowering the remote sender rate.

## Non-Goals

- Do not tune stale TUIC TCP pool behavior.
- Do not retune iperf3, sing-box, VPS direct path, or QUIC congestion control.
- Do not change TCP close/reap accounting unless a new test proves it is wrong.
- Do not lower the product default immediate byte budget as a blunt pacer change.

## Invariants

- Pending downlink bytes must not be dropped when smoltcp cannot accept them.
- A dirty TCP handle must remain dirty while app pending bytes or smoltcp tx queue
  pressure remains above the low watermark.
- Pending backlog may still force progress while local egress pressure is below
  the high watermark.
- Once the local TCP send queue reaches the high watermark, remote-payload
  immediate TUN flushes must defer to the timer/dirty-relay path.
- The change must keep diagnostics visible through `tun_flush_deferred`,
  `send_queue_max`, `may_recv_false`, `tun_drops`, and
  `tun_egress_feedback`.

## Acceptance

Local:

- Add a deterministic unit test for pressure-aware immediate-flush decisions.
- Keep existing downlink pacer, backpressure, close accounting, and parser tests
  passing.
- Run focused Rust tests plus script self-tests that cover the parser output.

VPS:

- Reverse-first P1 should no longer show TUN qdisc drops in the clean window.
- `send_queue_max` may reach high briefly, but sustained `may_recv_false` and
  `no_send_capacity` should decrease versus Knife14bk.
- `tun_flush_deferred` should increase for pressure deferrals.
- `terminal_pending_reap` must remain zero in the clean reverse-first P1 window.
- If throughput still stays near 10-20 Mbit/s with clean TUN drops and lower
  send-queue pressure, re-open architecture review instead of continuing suffix
  tuning.
