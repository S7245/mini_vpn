# Knife14eq Adaptive ACK Drain Plan

Date: 2026-07-07

## Design Tree

- Server path: rejected. Knife14ep direct `.27 -> .77` and `.33 -> .77`
  reverse baselines were healthy.
- QUIC path: rejected. safe1200 was active and QUIC loss/congestion/blocking
  stayed zero.
- Close/reap lifecycle: rejected. terminal pending, pending at close, and
  egress at close stayed zero.
- Drop-edge overrun: partially closed. Knife14ep cleared TUN drops.
- Local receive cadence starvation: selected. The fixed one-packet floor
  protected egress but left read/pending gaps and low average throughput.

## TDD Plan

1. Add a failing test showing observed clean egress progress must grow pressure
   ACK/window drain above the tiny floor at the flush edge.
2. Add a focused test showing the same adaptive floor clamps back to the tiny
   floor near the credit/high-water edge.
3. Add a focused test showing repeated no-progress/deferred feedback shrinks
   the adaptive floor back down.
4. Implement the smallest controller change:
   - add bounded adaptive ACK drain state to `DownlinkCreditController`;
   - grow it only from observed egress progress;
   - shrink it on no-progress or blocked pressure feedback;
   - let the relay pressure cap receive that adaptive floor;
   - keep tiny floor near high water and when no-progress streak is active.
5. Run focused tests first, then broader gates.

## Remote Plan

After local and `.27` focused gates pass:

1. Preflight `.33` sing-box, `.77` iperf3, and `.27` repo readiness.
2. Run one scoped safe1200 reverse-first P1 with `STOP_AFTER_REVERSE_FIRST_P1=1`.
3. Pull and parse the bundle.
4. If the suite fails, stop at evidence and a next modification plan before
   making further code changes.
