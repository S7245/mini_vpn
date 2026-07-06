# Knife14cb Egress-Progress-Clocked Accept Plan

Date: 2026-07-06

## Design Tree

1. Fixed cap at clean threshold is rejected.
   - It prevented overshoot but created sticky pending and receive-window
     starvation.
2. Removing the cap is rejected.
   - Knife14by/bz showed freer flushing can reintroduce TUN/qdisc drops and
     hard-pause pressure.
3. Progress-clocked acceptance is the next bounded algorithm.
   - Accept above the clean threshold only when the local send queue has
     actually drained since the last observation.
   - The extra allowance is one-shot credit and stays below the hard pause
     ceiling.

## Tasks

1. Add focused RED tests for observed-drain credit:
   - no previous drain behaves like Knife14ca;
   - observed queue decrease permits bounded extra accept;
   - credit is consumed and capped by hard pause headroom.
2. Add per-handle egress clock state to `SocketCtx`.
3. Extend `DownlinkFlushLimit`/diagnostics with drain-credit granted/used
   accounting.
4. Thread the clock through `flush_downlink` for both remote payload and
   dirty/timer pending flushes.
5. Update parser self-tests for new diagnostic fields if aggregate logs expose
   them.
6. Run local gates, review risk, commit/push, then run scoped VPS acceptance.

## Risk Checks

- Do not let credit accumulate unboundedly while a flow is idle.
- Do not consume credit when `send_slice` accepts fewer bytes than planned.
- Do not create a second source of pending loss on socket close/reset.
- Keep final lifecycle/drop summaries enabled for acceptance so any remaining
  pending/drop cannot be hidden behind the per-probe summary.
