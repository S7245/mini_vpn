# Knife14h10d16 Bounded QUIC UDP Send-Service Local Gate Results

Date: 2026-07-13

Source baseline: `a54fb17` with the existing uncommitted Knife14 worktree.
No commit was created. No VPS or macOS TUN test ran.

## Verdict

**FAIL / STOP before VPS.** The public Quinn `AsyncUdpSocket` adapter is
reachable and its deterministic safety contracts pass, but the first fixed
`48` wire-datagram / `2ms` hard-cooldown profile is not sufficient for the
required local `170 Mbit/s` capacity gate.

The real GSO-enabled loopback upload delivered exactly `32 MiB`, preserved the
payload pattern, and completed clean EOF, but measured only:

```text
sender_mbps=94.172
required_mbps>170
```

The target is `21.25 MB/s`. The original static calculation treated
`48 / 2ms` as `24,000` datagrams/s, or `30.72 MB/s` at `1280B`. The actual
implementation starts a fresh two-millisecond timer only after a batch is
exhausted. Runtime wake and driver-service overhead therefore sit on the
critical path; the static arithmetic did not establish the achieved service
period.

## What Passed

- runner red/green:
  - explicit forward-only discriminator failures now propagate out of the
    outer suite;
  - target-only mode no longer presents public exit-IP/fake-IP DNS checks as
    full-tunnel requirements;
  - receiver presence, zero TUN TX drop, and a fixed `16 MiB` formal QUIC-loss
    ceiling are fatal only in the explicit discriminator mode;
  - both runner self-tests and `bash -n` passed.
- bounded socket deterministic tests:
  - shared 48-datagram budget;
  - two pollers consume one aggregate budget rather than one budget each;
  - GSO segments are counted as wire-datagram equivalents;
  - an entire GSO transmit that cannot fit closes the batch and waits on a
    timer without busy self-wake;
  - inner `WouldBlock` is preserved without budget debit;
  - accepted bytes and transmit metadata are conserved.
- endpoint reachability:
  - production default remains Quinn direct service;
  - the explicit bounded policy injects the adapter through
    `Endpoint::new_with_abstract_socket` and exposes shared stats;
  - all TUIC pool connections created from that endpoint share the same
    socket/service state.

## Code Review Findings

1. **P0 — capacity invariant disproved.** `SendBatch` converts each exhausted
   batch into a new `now + 2ms` hard cooldown. The local gate's `94.172
   Mbit/s` proves this mechanism is necessary for burst control but not
   sufficient for the next throughput gate. It must not reach VPS acceptance.
2. **P1 — missing timing discriminator.** Stats record accepted bytes,
   datagrams, and rearm count but not scheduled deadline, actual wake time,
   total/max rearm lateness, or achieved datagrams/s. The current evidence
   cannot yet separate timer overshoot from unexpectedly small QUIC packet
   payloads or control-packet share.
3. **P1 — the runner loss budget is per-connection maximum, not pool
   aggregate.** The low-RTT summary exposes `max_lost_bytes_delta`; two pool
   connections can each remain below `16 MiB` while their total exceeds the
   intended endpoint loss budget. The next runner repair must consume an
   aggregate loss field or make the single-connection scope explicit.
4. **P2 — `gate_would_block` is not a pacing-activity counter.** Exact batch
   exhaustion is observed by the following `poll_writable` and can pace a run
   while this counter remains zero. Logs need separate synthetic rejection and
   timer-blocked/rearm signals.
5. The adapter remains a structurally sound deep seam: one production adapter,
   one mock adapter, no payload queue/copy, bounded state, delegated receive
   behavior, and timer-backed liveness. Structural score is `8/10`; acceptance
   sufficiency is `0/10` until the local throughput gate passes.

## Proposed Next Plan — Requires Confirmation

Do not alter the 48/2ms constants or run VPS yet.

1. Add a measurement-only red/green tracer to the existing adapter stats:
   `batch_closes`, `timer_blocked_polls`, total/max rearm lateness, and achieved
   accepted datagrams/bytes per elapsed interval. Print the full snapshot in
   the failing 32 MiB test.
2. Re-run only the deterministic adapter tests and the real 32 MiB upload.
   Compare two falsifiable branches:
   - timer branch: mean actual rearm period materially exceeds 2ms;
   - packetization branch: rearm period is near 2ms but accepted bytes per
     datagram is materially below the capacity assumption.
3. If timer delay is selected, re-evaluate the architecture instead of tuning
   the constant blindly. Compare a narrow Quinn pacer burst-cap seam (preserves
   Quinn's rate/debt model) against a deadline/debt service that bounds each
   wake without adding one full sleep after every batch. Perform code-level
   capacity math and a new red/green test before implementation.
4. If packetization is selected, derive the fixed service budget from observed
   minimum wire payload and control share, then repeat the same local
   sufficiency gate. This is a new plan decision, not an automatic parameter
   adjustment.
5. Only after exact delivery, clean EOF, and `>170 Mbit/s` pass locally may the
   full library/harness/concurrency/UDP gates and one same-window control + mini
   forward P1 be reconsidered.

D16, TUN/QUIC MTU, pool, QUIC windows, chunk, Cubic, and self-wake remain
frozen. P8, UDP/live-streaming, fake-IP DNS, and rearm remain unspent.
