# Knife15 M2 Replacement Current-Service Handoff Architecture Spec

Date: 2026-08-12

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; PAIRED M2 QUALIFICATION
REQUIRED; FORMAL M2 AND M3 BLOCKED**

Failure evidence:
`docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-formal-failure-results.md`.

## Decision

Deepen the existing auxiliary generation-replacement module with one exact
**replacement current-service handoff** transaction.

Before any successor handshake or service proof, the generation slot must
hold its ownership mutex, verify the exact stable transport identity and
logical generation, sample that current Quinn connection's positive cwnd,
and publish the monotonic replacement floor as:

```text
required_floor = max(previous_owned_floor, current_generation_cwnd)
```

The small handoff result exposes both the observed current cwnd and required
floor. The successor then uses the existing exact sequential ACK-owned service
turns until its current cwnd reaches that floor. The existing install CAS
rechecks the latest generation-owned floor before transferring current
ownership.

This is a handoff of already observed congestion service, not a configured
throughput promise. It closes the missing replacement path without changing
the established path-reset publication path.

## Why This Seam

The failed generation had current `cwnd=361,778B`, but replacement read only
the stale `26,338B` floor created when that generation was installed. The
floor was updated only by `reset_path_if_owned()`. Direct replacement for
`forward_qualification_degraded` bypassed that publication seam and installed
a successor at `26,338B`.

Sampling outside the slot is rejected because identity and cwnd could describe
different logical generations. Sampling only at install is too late because
the successor proof has already completed. A fixed minimum, a minimum proof
round count, or a multiplier is parameter tuning and cannot express the exact
capacity being surrendered.

`TcpPoolGenerationSlot<Connection>` is the deep module. Its interface is one
replacement-preparation operation; its implementation hides lock ownership,
identity/generation validation, the Quinn sample, positive-value validation,
monotonic publication, and the handoff result. The replacement caller does
not manipulate the generation field directly.

Architecture/refactoring score: **10/10 target**. The named transformation is
an Extract Method plus Move Method into the generation slot, using the
existing branch-by-abstraction seam. No new public API or adapter is added.

## Goals

1. Prevent direct auxiliary replacement from regressing below the exact
   current generation's observed congestion service.
2. Keep sample, identity check, and floor publication one slot-owned
   transaction.
3. Preserve the existing exact ACK/loss/path-owned successor proof and install
   CAS.
4. Fail closed on stale identity or zero current cwnd.
5. Log observed current cwnd separately from the inherited required floor.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool size, QUIC windows, chunk, Cubic, GSO,
  Endpoint rate/burst, self-wake, recovery bounds, workload, or SLIs.
- Do not add a cwnd, rate, duration, round, retry, or byte constant.
- Do not use current cwnd to mutate Quinn directly; the successor must prove
  the floor through the existing transport-native turns.
- Do not migrate or replay business streams, retain another eligible lane,
  probe a Target, or relax the receiver-zero rule.
- Do not claim that a cwnd handoff proves arbitrary WAN throughput.

## Invariants

1. The handoff belongs to one exact stable identity and logical generation.
2. The slot mutex covers identity verification, current cwnd sampling, and
   monotonic floor publication.
3. Zero cwnd cannot authorize a replacement.
4. A stale caller cannot read or publish a newer generation's handoff state.
5. The required floor never decreases.
6. Replacement cannot begin its handshake without a successful handoff.
7. The successor proof remains exact packet/path/ACK owned, loss-fail-closed,
   and inside the unchanged five-second whole-transaction deadline.
8. Install rechecks the latest floor; insufficient proof cannot take current
   ownership.
9. Failed handoff or proof leaves the predecessor current and preserves the
   existing attempt-local qualified fallback.
10. Existing streams retain their predecessor drain ownership.

## Capacity And Reachability Gate

The failed formal short-forward rate was `14,221,126 bit/s`, or
`1,777,640.75 application B/s`. Its minimum useful Target interval was
`131,072B`.

At about `178ms` RTT, the installed `26,338B` floor represents only about
`1.184 Mbit/s` of one-window flight rate. The exact predecessor current cwnd
was `361,778B`, about `16.26 Mbit/s` at the same RTT. This is not a throughput
prediction; it proves that the existing floor omitted an order of magnitude
of reachable forward service.

Starting at `12,000B`, exact slow-start turns can reach the observed floor in
roughly five sequential RTT-owned rounds:

```text
12,000 -> about 26k -> 52k -> 104k -> 208k -> >=361,778B
```

The rounds and bytes are dynamically determined, share the existing five
second deadline, and occur before any Target request. Endpoint application
capacity remains approximately `239 Mbit/s`, far above the frozen workload.

End-to-end hot path remains:

```text
new Target open -> pool admission -> replacement preparation handoff
-> successor handshake -> exact sequential service proof
-> generation/activity install CAS -> TUIC Connect -> D16 writer
-> Quinn STREAM -> Endpoint pacing -> Exit -> Target
```

The change is intended to be sufficient only for one fresh qualification
discriminator. A real Mac/Exit run remains required.

## Old-Path Audit

Path-reset floor publication, successor certificate admission, black-hole
qualification, service-normalized selection, transport-write flight
ownership, authentication ownership, failure fallback, predecessor drain,
reverse-gap ACK reinforcement, D16, TUN, Endpoint pacing, MTUD, Cubic, GSO,
and self-wake all remain active. No old writer or local egress path is bypassed.

## Failure Discriminators And Stop Rule

- A replacement log with `observed_current_cwnd > old_certificate_floor`, a
  multi-round proof reaching the new floor, and no receiver-zero selects the
  mechanism.
- Stale identity or zero cwnd must reject preparation without a handshake.
- Loss, path change, no progress, deadline, or install-CAS mismatch must fail
  closed and retain the predecessor.
- A receiver-zero after a successor proves the exact handoff floor rejects
  this mechanism; do not tune or repeat unchanged.
- Any D16, Endpoint, TUN, UDP, cleanup, or lifecycle regression stops the
  stage for root-cause repair.
- Local 32MiB capacity at or below `170 Mbit/s` is an architecture failure;
  constants must not be tuned.
