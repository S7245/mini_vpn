# Knife14ep Predictive Drop-edge Controller Plan

Date: 2026-07-07

## Design Tree

- Server path: rejected for this stage. Direct `.27 -> .77` and `.33 -> .77`
  reverse baselines were healthy in Knife14eo.
- QUIC path: rejected for this stage. safe1200 disabled PLPMTUD, QUIC
  loss/congestion/blocking stayed zero, and stream frames continued flowing.
- close/reap lifecycle: rejected for this stage. terminal pending, pending at
  close, and egress at close stayed zero.
- Local projected egress overrun: selected. Knife14eo showed a TUN TX drop and
  `send_queue + pending` overrun after projected payload debt.

## TDD Plan

1. Add a focused failing test showing projected relay read credit near the local
   egress edge must shrink below the old pressure floor while remaining
   non-paused when bounded staging has room.
2. Add or adjust a focused controller test showing repeated headroom/debt
   feedback compresses flush budget below the old pressure floor.
3. Implement the smallest controller change:
   - introduce an ACK/window drain floor smaller than the old pressure floor;
   - let pressure-region relay credit use that floor when local egress
     headroom is gone;
   - let controller shrink read/flush ceilings below the old pressure floor
     during headroom or debt recovery;
   - preserve hard pause only for hard local staging/TUN feedback cases.
4. Run focused tests, then broader gates.

## Remote Plan

After local and `.27` focused gates pass:

1. Preflight `.33` sing-box, `.77` iperf3, and `.27` binary/suite readiness.
2. Run one scoped safe1200 reverse-first P1 with `STOP_AFTER_REVERSE_FIRST_P1=1`.
3. Pull and parse the bundle.
4. If the suite fails, stop at evidence and a next modification plan; do not
   make random follow-up changes.
