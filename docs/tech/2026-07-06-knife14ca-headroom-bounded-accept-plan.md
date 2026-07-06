# Knife14ca Headroom-Bounded Remote Accept Plan

Date: 2026-07-06

## Design Tree

1. Static threshold-only tuning is rejected.
   - Knife14bx soft high avoided TUN drops but left too much flush deferral.
   - Knife14by hard cap lowered deferral but introduced drops.
   - Knife14bz midpoint still overshot and regressed.
2. Post-send immediate flush gating is too late.
   - `send_slice_max_accepted=65536` and `send_queue_max=917489` show the local
     queue can be pushed to the hard-pause region before the pacer decides to
     defer a TUN flush.
3. Pre-send headroom is the next narrow code lever.
   - Cap each pending flush by remaining clean tx_queue headroom.
   - Keep unaccepted bytes in `downlink_pending`.
   - Let existing dirty/backpressure machinery reattempt after TUN egress drains.

## Tasks

1. Add a focused RED test for bounded flush length under send_queue pressure.
2. Extend `TcpDownlinkDiag` with headroom-limited accounting and expose it in
   aggregate logs.
3. Change `flush_downlink` to compute an egress-aware send limit before
   `send_slice`.
4. Thread `DownlinkBackpressureConfig` through callers instead of adding new
   runtime knobs.
5. Run local gates, review for lifecycle regressions, then commit/push.
6. Run the scoped VPS acceptance and record results/learnings in a separate
   docs commit.

## Risk Checks

- Pending bytes must remain pending, not silently dropped.
- Close/reap must still account for pending when the socket becomes terminal.
- Timer-driven dirty flush must use the same headroom cap as remote payload
  handling, otherwise timer flush can reintroduce overshoot.
- Diagnostic counters must not become hot-path noisy logs; aggregate metrics
  are enough for the first patch.
