# Knife14ce Drop-Aware Egress Credit Spec

Date: 2026-07-06

## Stage Goal

Make TCP downlink egress credit aware of TUN egress drops. After a TUN
`tx_dropped` delta is observed, mini_vpn must stop treating the next
`send_queue` decrease as clean local egress drain. The decrease should first
repay a bounded drop debt; only later clean drain may grant extra credit above
the clean tx-queue headroom.

## Background

Knife14cd restored clean reverse-first P1 throughput with default TUIC TCP
pool=2:

- reverse receiver: `179 Mbit/s`
- no data starvation
- no clean-window QUIC loss/congestion/blocking
- no `send_slice` zero/error
- no TUN flush failure
- no terminal pending reap

The same run still exposed local egress instability:

- `tun_tx_dropped_delta=6754`
- `runtime_tun_egress drop_events=8 drop_delta_total=6754`
- `final_egress_at_close bytes=892928 drain_candidate_events=1`
- `send_queue_max=892928`, matching the credit spend hard edge
- large drain-credit and hard-edge counters

This points at a local algorithmic issue: the egress clock currently grants
credit from any observed `send_queue` decrease. After a TUN drop, that decrease
may be caused by packet loss rather than successful local delivery, so granting
fresh credit immediately can refill the smoltcp tx queue back to the hard edge
and repeat the drop cycle.

## Non-Goals

- Do not increase TUIC TCP pool beyond the Knife14cd default pool=2.
- Do not reopen stale pool, sing-box auth/time/config, iperf3, QUIC
  loss/congestion, TUN queue length, or egress pacer tuning unless new evidence
  contradicts Knife14cd.
- Do not solve this by changing only static high/low thresholds.
- Do not hide final pending/close/reap signals; they must remain explicit.

## Invariants

- Clean headroom under `tx_queue_flush_threshold` remains usable even while drop
  debt exists.
- Drain credit above clean headroom is one-shot, bounded, and never spends past
  the hard-edge guard.
- A positive TUN drop feedback event clears stale per-flow drain credit before
  any further credit can be planned.
- Observed `send_queue` decreases repay global drop debt before they can grant
  new drain credit.
- Drop-aware behavior is observable in both aggregate `tcp-downlink-flush`
  diagnostics and close-line accounting.

## Acceptance

Local:

- Focused tests prove:
  - existing drain credit cannot be spent immediately after a TUN drop;
  - observed queue drain pays drop debt before granting new credit;
  - clean headroom is not reduced by drop debt; and
  - diagnostics include drop-credit debt, paid, and blocked counters.
- Existing backpressure, lifecycle, parser self-tests, full Rust tests, harness
  tests, release build, and clippy remain green.

VPS:

- Run a scoped clean reverse-first P1 with default pool=2.
- Preserve high throughput; regression back to the 10-20 Mbit/s class is a
  failed stage.
- `tun_tx_dropped_delta` should be zero or materially lower than Knife14cd's
  `6754`.
- `final_egress_at_close` should no longer sit at the credit hard edge with
  active send-capable drain candidate backlog.
- New drop-credit counters should show whether drop debt activated and whether
  subsequent local drain repaid it.

## Progress Target

If the local gates pass and the VPS run preserves high throughput while
removing or materially reducing TUN drops and final hard-edge close backlog,
Knife14 can move from about `88%` to at least `90%`. If throughput regresses or
drop debt remains active without reducing drops, stop and re-evaluate the
downlink architecture before another suffix patch.
