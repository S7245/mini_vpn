# Knife15 M2 Successor Forward-Service Inheritance Architecture Spec

Date: 2026-08-10

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED QUALIFICATION IS REQUIRED BEFORE FORMAL M2; M3 REMAINS BLOCKED**

Failure evidence:
`docs/tech/2026-08-10-knife15-m2-successor-forward-service-inheritance-formal-failure-results.md`.

## Decision

Deepen the existing auxiliary generation-replacement module with one exact
**TUIC TCP successor forward-service inheritance** contract.

When the recovery monitor is about to apply `path_changed()` to the current
logical generation, the generation slot first records the exact pre-reset
positive cwnd as its replacement service floor. Ownership is conditional on
the same stable transport identity and logical generation; a stale monitor
handle cannot publish evidence or reset a newer generation.

When that generation is later replaced, the successor must always complete at
least one existing exact successor service turn. If an inherited floor exists,
it completes sequential current-cwnd turns until its current cwnd reaches that
floor. Every turn retains the existing exact packet/path/ACK ownership, and all
rounds must remain on one path generation. The existing five-second whole
replacement deadline bounds handshake plus proof; loss, path change,
connection close, deadline, or a successful round without cwnd progress fails
closed.

The installed successor certificate records its final exact path generation
and post-proof cwnd. That floor also becomes the generation's replacement
floor, so a later stale-certificate replacement cannot silently regress below
service already proved by its predecessor.

No Target request or business payload exists before install. Existing
identity/activity CAS, predecessor drain, and attempt-local qualified-only
fallback remain authoritative.

## Why This Mechanism

Formal M2 rejected the earlier assumption that one current-cwnd flight is
sufficient. The predecessor had demonstrated `41,301B` immediately before its
one-shot recovery reset, while the successor installed after one turn at only
`26,424B`; the same Ready successor later lost the first Target receiver
interval.

The required floor is therefore observed generation state, not a configured
threshold. Sequential turns reuse the exact transport proof and the existing
transaction deadline. They neither change Cubic nor predict bandwidth; they
only prevent a replacement from taking forward ownership below the capacity
the exact predecessor generation had to surrender at recovery.

Rejected alternatives:

- a larger fixed turn, cwnd minimum, round count, or SLI relaxation is
  parameter tuning;
- periodic keep-warm traffic adds a permanent timer and background load;
- Target canaries, request duplication, or replay violate transparent TCP
  ownership;
- installing the cold successor and warming it with business traffic repeats
  the failed architecture;
- retaining two eligible generations or migrating streams expands the frozen
  pool/lifecycle contract.

## Goals

1. Preserve the exact predecessor generation's observed pre-reset forward
   service across an isolated auxiliary replacement.
2. Keep proof transport-native, exact-path, loss-fail-closed, and bounded by
   the existing replacement transaction.
3. Reject stale monitor ownership and prevent a path reset whose service floor
   was not durably associated with the current generation.
4. Preserve initial-generation availability, certificate anti-churn,
   replacement fallback, predecessor drain, and TCP/UDP/TUN semantics.
5. Expose inherited floor, proof rounds, aggregate exact bytes, and terminal
   reason in diagnostics.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool size, QUIC windows, chunk, Cubic, GSO,
  Endpoint rate/burst, self-wake, recovery bounds, workload, or SLIs.
- Do not add a configured cwnd, rate, byte, elapsed, round-count, or retry
  parameter.
- Do not expand the pool, replace primary slot 0 through this seam, migrate
  streams, replay payload, or probe a Target.
- Do not claim that inherited cwnd is a bandwidth promise or sufficient proof
  for general WAN throughput.

## Deep Module And Dependency Direction

`TcpPoolGenerationSlot` owns the small scalar interface:

```text
record current generation's pre-reset service floor(expected identity, cwnd)
read current generation's replacement service floor(expected identity)
```

It hides monotonic max, positive-value validation, identity/generation CAS, and
the current-versus-draining distinction. `TcpPoolGeneration` owns the scalar
floor beside its certificate and lifecycle state.

`TuicUpstream` remains the outer Quinn adapter. It samples Quinn immediately
before reset, asks the slot to record exact ownership, applies the reset only
after success, then later executes the isolated replacement proof. A private
proof helper hides sequential turns, deadline, same-path aggregation, cwnd
progress, and terminal errors behind one result.

No new trait is introduced because Quinn is the only concrete adapter. The
policy stores no Quinn object and imports no packet-number or congestion
controller internals.

Clean Architecture score: **10/10**. Refactoring score: **10/10** for
branch-by-abstraction through the existing reset and replacement seams.
System Design score: **10/10** for explicit capacity, latency, lifecycle,
observability, and acceptance boundaries. DDIA score: **10/10** for exact
identity/version ownership, monotonic bounded state, atomic install,
backpressure preservation, and fail-closed fault handling.

## Invariants

1. A floor belongs to one exact stable identity and logical generation.
2. Only a positive pre-reset cwnd or a successful successor proof can publish
   a floor.
3. Repeated evidence is monotonic `max`; it can never lower the requirement.
4. A stale identity cannot record or read a current generation's floor.
5. `path_changed()` is applied only after the exact current generation accepts
   the pre-reset floor.
6. Every replacement completes at least one exact service turn.
7. With a floor, proof ends only after current `cwnd >= inherited_floor`.
8. All successful turns share one path generation; any turn loss or path
   change is terminal.
9. A successful turn that does not increase cwnd while still below the floor
   is terminal rather than an unbounded retry.
10. Handshake and all proof turns share the unchanged five-second replacement
    deadline.
11. Install still requires exact predecessor identity/activity CAS, an atomic
    recheck that the successor's proved floor satisfies the latest slot-owned
    predecessor floor, and one global drain permit.
12. Failure leaves the predecessor current and invokes no same-open second
    replacement or payload replay.

## Capacity And Reachability Gate

Formal short-forward offered rate:

```text
10,270,678 bit/s = 1,283,834.75 application B/s
one complete receiver interval = 131,072B minimum useful progress unit
```

The failed successor installed at `26,424B`; the exact predecessor
pre-reset floor was `41,301B`. Under ideal one-RTT slow-start service:

```text
26,424 * (1 + 2 + 4) = 184,968B
41,301 * (1 + 2 + 4) = 289,107B
```

The arithmetic is a plausibility bound, not a throughput promise. The
important reachability change is that the replacement cannot install at
`26,424B` when exact generation-owned evidence requires `41,301B`; a second
current-cwnd proof turn is reachable and grows the successor before any
business open. Endpoint application capacity remains approximately
`239 Mbit/s`, far above the offered rate.

End-to-end hot path remains:

```text
remote TUIC STREAM -> Quinn RecvStream -> D16 relay reader
-> smoltcp socket -> iface.poll -> flush_tx -> TUN egress

new Target open -> TcpPoolAdmission -> existing ReplaceAuxiliary
-> TUIC handshake -> inherited forward-service proof
-> generation/activity CAS -> Connect -> D16 writer
-> Quinn STREAM -> Endpoint pacing -> Exit -> Target
```

This change is intended to be sufficient only for the next exact short-flow
qualification discriminator. Formal M2 remains required.

## Old-Path Audit

Transport-write flight ownership, authentication/service-turn packet
ownership, service certificate admission, forward black-hole qualification,
service-normalized admission, failure fallback, predecessor drain, startup
scheduling, writer recovery/path reset, reverse-gap ACK reinforcement, D16,
Endpoint pacing, MTUD, Cubic, GSO, and self-wake all remain active. No local
egress or writer path is bypassed.

## Failure Discriminators

- exact pre-reset record plus `proof_rounds > 1`, final cwnd at/above inherited
  floor, and no next receiver-zero: retain the architecture;
- stale identity at reset: skip the reset and log ownership rejection;
- turn loss/path change/no progress/deadline: fail replacement, retain the
  predecessor, and use the existing qualified-only fallback;
- one round already reaches the dynamic floor: install without artificial
  extra work;
- a receiver-zero recurs after a successor reaches a nonzero inherited floor:
  reject this mechanism as sufficient; do not tune or repeat unchanged;
- D16, Endpoint, TUN, UDP, route, cleanup, CAS, or predecessor lifecycle
  regression rejects implementation.

## TDD And Stop Rule

The scalar RED requires exact identity ownership, monotonic floor publication,
stale rejection, and clearing on a transport reconnect. The transport RED
uses a real local Quinn pair and a dynamic floor above one initial service
turn; old code can complete only one round, while GREEN must complete multiple
sequential exact turns on one path and reach the inherited floor.

Additional slices cover one-round compatibility, no-progress failure,
same-path aggregation, path-reset ordering, certificate/floor transfer, and
replacement CAS/fallback regression.

Expected RED may enter minimal implementation. Any static threshold, new
retry/timer, path reset without owned floor, unbounded loop, frozen-value
change, local 32MiB capacity at or below `170 Mbit/s`, or unexpected repair
failure stops implementation for analysis. After focused/full gates and code
review pass, take one qualification run before another formal M2. The
qualification remains non-acceptance and must not be repeated or tuned
unchanged.
