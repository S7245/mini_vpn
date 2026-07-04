# Knife14ax plan - tx-queue-aware downlink backpressure

Date: 2026-07-04

## Stage Goal

Close the missing feedback edge where smoltcp tx queue saturation is invisible
to `global_rx_paused`.

## Design Tree

1. Increase TUN queue length.
   Rejected. Knife14ak already made this an attribution direction, not a
   product fix, and Knife14aw now shows the missing local control signal before
   the kernel queue.

2. Tune egress pacing again.
   Rejected. Knife14ar already corrected close-safe pacing defaults, and
   Knife14aw shows the backpressure decision resumes when `downlink_pending`
   empties even if smoltcp tx queue remains full.

3. Preserve terminal pending longer.
   Rejected for this slice. The clean reverse-first close was terminal
   `Closed && may_send=false`; preserving undeliverable bytes would not explain
   why throughput was low before close.

4. Include smoltcp tx queue occupancy in downlink backpressure hysteresis.
   Selected. It directly addresses `pending_total=0` with
   `send_queue_max=1048576`, keeps existing watermarks, and remains local to the
   reverse/downlink path.

## Tasks

1. Record Knife14aw failed VPS result and learning/error memory.
2. Add a failing focused test for tx-queue pressure with empty app pending.
3. Extend downlink pressure stats with smoltcp tx queue pressure.
4. Update the main-loop `global_rx_paused` decision and diagnostic log.
5. Extend low-RTT probe parser/self-test for tx-queue pressure fields.
6. Run local regression gates.
7. Review risk, commit, push.
8. Rerun scoped reverse-first VPS acceptance.

## Regression Checks

- Older `tcp-downlink-backpressure` logs without new fields remain parseable.
- `downlink_pending` still forces pause/resume exactly as before.
- tx queue pressure does not depend on private smoltcp internals.
- Multiple dirty sockets aggregate max and total pressure without overflow.
- No change is made to close/reap, egress pacing, TUIC pool, TUN queue length,
  iperf3, or sing-box behavior.
