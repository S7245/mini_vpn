# Knife15 M1 stream-ACK-qualified Endpoint rebind implementation plan

Date: 2026-07-22

## Constraints

Preserve the accepted Endpoint rebind mechanism and all frozen data-plane,
recovery, workload, and SLO values. Make the smallest deepening that exposes
the exact business-stream delivery signal; do not tune the two-second floor or
add another recovery mechanism.

## Tasks

1. **Freeze the artifact diagnosis.** Verify archive SHA/source/runner/binary,
   operation sequence, baseline/direct continuity, receiver-zero intervals,
   rebind timing, physical controls, Endpoint conservation, and cleanup.
2. **Add the RED invariant.** Prove that ACK progress resets recovery stall
   age without clearing a still-Pending writer. Prove the recovery state does
   not rebind such a writer.
3. **Expose Quinn stream progress.** Add a read-only cloneable handle backed by
   quinn-proto's per-stream written and acknowledged byte accounting. Test it
   through a real loopback acknowledgement.
4. **Deepen pressure ownership.** Replace connection-wide Pending aggregation
   with per-writer/per-stream episodes. Keep the write hot path atomic and use
   a bounded connection-local registry for lifecycle and 250ms sampling.
5. **Qualify recovery.** Require both Pending duration and exact-stream ACK-
   stall duration to reach the unchanged RTT bound. Cover every current writer
   in one rebind and retain no-RX plus authenticated set-wise recovery.
6. **Improve evidence.** Extend rebind logs with connection, writer, stream,
   episode, acknowledged bytes, and total Pending age while preserving the
   existing prefix and trigger label.
7. **Run focused and complete gates.** Execute TUIC recovery/pressure tests,
   full root, Quinn/proto and docs, release, Clippy, Knife15/Knife14 shell,
   formatting/diff, changed-content secret scan, and exact 32 MiB capacity.
8. **Review and hand off.** Audit locking, atomics, stream lifetime, every
   generic/native reachability path, failure conservatism, rebind de-
   duplication, logging, and TUN/UDP/D16 regression risk. Record results and
   prepare one fresh user-run HK M1.

## Stop rules

- Expected focused RED may enter the minimal implementation.
- Unexpected regression failures must be diagnosed and repaired without
  relaxing an invariant.
- Local 32 MiB throughput `<=170 Mbit/s` rejects the architecture; constants
  must not be tuned.
- No macOS TUN is run by the agent. After all local gates and review pass, push
  the repair and request the existing user-operated M1 flow.
