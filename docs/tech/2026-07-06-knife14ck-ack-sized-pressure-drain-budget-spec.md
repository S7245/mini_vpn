# Knife14ck ACK-Sized Pressure Drain Budget Spec

Date: 2026-07-06

## Goal

Fix the pressure TUN RX drain budget after Knife14cj proved the drain timing is
correct but the packet budget is too small.

Knife14cj produced `attempts=252 packets=5292 budget_exhausted=252
would_block=0`. Source counters showed pre-payload and timer-maintenance drains
engaged, but every attempt stopped at the 21-packet MTU-derived budget while
local TUN egress drops continued.

## Non-Goals

- Do not set `MINI_VPN_TUN_RX_DRAIN_BUDGET` to a larger default.
- Do not make TUN RX drain unconditional.
- Do not change TUIC pool, sing-box, iperf3, receive-window, or TUN queue
  length.
- Do not remove close/reap/pending accounting.

## Invariants

- Explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET>0` keeps its current operator
  override behavior.
- Default `MINI_VPN_TUN_RX_DRAIN_BUDGET=0` remains pressure-gated.
- Adaptive pressure drain budget is still derived from the existing credit
  guard, but packet count uses an ACK-sized packet estimate instead of MTU.
- The derived budget has a hard packet cap to prevent unbounded hot-path
  scanning.
- Small guard spans still produce at least one packet of drain when pressure is
  eligible.

## Acceptance

Local:

- Focused tests prove the adaptive budget is ACK-sized and capped.
- Existing pre-payload, maintenance, pressure-credit, drop-credit, harness, and
  clippy gates pass.

VPS:

- Same clean reverse-first P1 suite.
- Expected signal: pressure drain reaches `would_block` for at least some
  attempts, TUN egress drops reduce materially, and reverse throughput leaves
  the 10-20 Mbit/s class.
- If budget reaches `would_block` but drops persist, stop ACK budget work and
  inspect local TUN egress scheduling/flush cadence.
