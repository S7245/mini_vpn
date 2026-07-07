# Knife14cs Broader TCP Regression Plan

Date: 2026-07-07

## Stage Plan

1. Keep code unchanged from Knife14cr commit `3a35ee1` unless the broader run
   exposes a new parsed failure class.
2. Preflight `.33` for active sing-box, synchronized clock, and current-window
   TUIC `fail auth`.
3. Run `.27 -> .33 -> .77` with the existing suite:
   `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=0`,
   `PARALLEL_SET="1 2 4 8"`, `DURATION=30`, MTU `1200`,
   `SERVER_EVIDENCE_CHECK=0`, explicit `.33` exit SSH variables, and
   `BUILD_RELEASE=0` to reuse the accepted binary if the remote hash matches.
4. Pull and extract the bundle locally.
5. Parse reverse-first P1, standard P1, and the full sweep for throughput
   shape, pending/close/reap, TUN drop, backpressure, send-slice, TUN flush,
   and QUIC signals.
6. If it passes, record the result, update learning memory, and commit/push
   the stage.
7. If it fails, record the failure in `.learnings/ERRORS.md`, write the repair
   hypothesis, then implement the smallest code patch needed by the parsed
   evidence.

## Parse Checklist

- source hash and binary hash on `.27`
- direct `.27 <-> .77` and `.33 <-> .77` baselines
- reverse-first P1 sender/receiver Mbit/s and interval shape
- standard P1 sender/receiver Mbit/s and interval shape
- full sweep per-parallel sender/receiver Mbit/s and interval shape
- `timer_active_flow_attempts`
- TUN drops and downlink backpressure pause/resume edges
- pending at close, egress at close, terminal pending reap, terminal late
  remote payload
- send-slice zero/errors and TUN flush failures
- QUIC loss/congestion/blocking
- `.33 fail auth` current-window check
- `.27` cleanup state
