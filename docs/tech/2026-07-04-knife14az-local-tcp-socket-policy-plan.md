# Knife14az plan - local TCP socket policy

Date: 2026-07-04

## Design Tree

Accepted evidence from Knife14ay:

- The clean reverse-first failure is not caused by terminal pending hiding
  pre-close bytes.
- The local TCP socket closed before the late pending bytes were reaped.
- Remote input was available, but local smoltcp downlink drain accepted small
  bursts and built a large tx queue.

Branches for this stage:

1. Local smoltcp Nagle/delayed-ACK policy is adding avoidable virtual-link
   latency and limiting downlink drain.
2. Listener construction applies a future policy, but rearm silently restores
   smoltcp defaults after the first connection.
3. The policy is applied correctly, but VPS acceptance still exposes a deeper
   smoltcp window/queue behavior that needs architecture review.

Rejected branches unless new evidence appears:

- stale TUIC pool slots;
- iperf3 or sing-box service behavior;
- egress pacer immediate budget;
- TUN qdisc/syscall failure;
- clean-window QUIC loss/congestion.

## Task Plan

1. Add the stage spec and plan docs.
2. Add a red test for listener socket local TCP policy.
3. Implement the smallest socket-policy helper for listener construction.
4. Add a red test for rearm preserving the same socket policy.
5. Apply the helper during rearm.
6. Run focused and broad local gates.
7. Review risk and update `.learnings/LEARNINGS.md`; update
   `.learnings/ERRORS.md` only if a failure changes future behavior.
8. Commit and push the local code/docs task.
9. Run scoped `.27` reverse-first VPS acceptance on the pushed commit.
10. Parse the bundle, write the result doc, update learning memory, then commit
    and push the results task.

## Risk Controls

- The code change is limited to smoltcp socket options for local virtual-link TCP
  sockets.
- No close/reap semantics change in this task.
- No queue size, pacer, pool, or server config change in this task.
- If local gates fail, analyze the failing path and state a correction plan
  before changing behavior code again.
- If VPS acceptance still fails, record the new limiter and ask for confirmation
  before the next behavior patch.

