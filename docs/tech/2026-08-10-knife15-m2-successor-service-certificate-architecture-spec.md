# Knife15 M2 Successor Service Certificate Architecture Spec

Date: 2026-08-10

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION IS REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Failure evidence:
`docs/tech/2026-08-10-knife15-m2-successor-service-certificate-qualification-failure-results.md`.

Local result:
`docs/tech/2026-08-10-knife15-m2-successor-service-certificate-local-results.md`.

## Decision

Deepen TCP pool admission with one transport-native **TUIC TCP successor
service certificate**. A freshly authenticated auxiliary successor that
passes its existing exact service turn records:

```text
stable transport identity
logical pool generation
service-turn path generation
post-turn proved cwnd floor
```

The certificate is immutable for that logical generation. Before new-open
admission, the outer Quinn adapter supplies the current identity, path
generation, cwnd/RTT, and existing black-hole evidence as scalar observation.
The inner admission policy classifies a certified successor as service-ready
only while identity and path generation still match and current cwnd is at
least its own proved floor.

A certified auxiliary whose service evidence is stale is not requalified in
place. It enters the existing bounded fresh-generation replacement seam:
QUIC/TUIC handshake, one exact successor service turn, atomic identity and
activity CAS install, then predecessor drain. Existing streams remain on the
predecessor; no Target open or business payload exists before the new
generation is installed.

Initial pool generations have no successor certificate and retain existing
`Unknown` availability behavior. The certificate is not a general health
score, bandwidth promise, configured threshold, or congestion-event
blacklist.

## Why This Mechanism

The rejected source demonstrates that a service turn can be true when a
successor installs and stale when a later new open arrives. PLPMTUD black-hole
qualification cannot express that state: the failing generation stayed at
zero black holes while its cwnd fell `24,800 -> 17,360B`.

Running the service turn on a current generation is rejected. The Quinn turn
adopts existing in-flight Data packets, emits full-MTU PING/PADDING carriers,
and has no live-generation quiescence/cancellation CAS. On a busy generation
it could mix maintenance ownership with unrelated business streams; even an
idle special case would create a second readiness lifecycle. Fresh auxiliary
replacement already provides the required isolation and bounded predecessor
ownership.

Treating every congestion event as degradation is also rejected. Ordinary WAN
loss is expected and a monotonic event counter would churn connections after
healthy high-window recovery. The certificate compares current service only
with the exact floor that this generation proved, with no constant or ratio
knob.

## Goals

1. Prevent a successor whose current path service fell below its own exact
   install proof from owning a later new Target open.
2. Preserve the existing forward-qualification, normalized-load, generation
   replacement, predecessor-drain, and failure-fallback contracts.
3. Keep policy Quinn-independent by passing only scalar observations into the
   deep admission module.
4. Preserve availability for initial/unknown evidence and bounded fallback
   when replacement is unavailable or fails.
5. Produce exact certificate state and replacement reason in selection logs.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, configured pool, QUIC windows, chunk, Cubic,
  GSO, Endpoint rate/burst, self-wake, recovery bounds, startup priority,
  workload, or SLIs.
- Do not add a configured cwnd/rate/elapsed threshold, multiplier, hysteresis,
  timer, or retry.
- Do not replace primary slot 0, expand the pool, keep another eligible warm
  generation, migrate streams, or replay payload.
- Do not run a service turn on an installed current generation.
- Do not claim formal-M2 or general WAN throughput sufficiency.

## Deep Modules And Dependency Direction

`TcpPoolAdmission` owns certificate classification, categorical ordering, CAS
preparation, and the pure decision to reserve current or replace auxiliary.
Its interface consumes scalar path observations; it does not import packet
numbers, Quinn connection objects, or async transport operations.

`TcpPoolGeneration` owns the immutable optional certificate beside the exact
transport identity/activity it qualifies. `TuicUpstream` remains the outer
adapter: it samples Quinn, executes the already-existing replacement action,
and installs a successor only after the exact service turn and generation CAS
succeed.

The deletion test confirms this is a deepening rather than another shallow
module: removing it would spread certificate identity, path, floor, admission,
replacement, and log rules across observation, open, and replacement callers.
No new trait is introduced because there is one concrete Quinn adapter.

Clean Architecture score: **10/10**. Scalar policy remains inward and Quinn
remains an outer detail. Refactoring score: **10/10** for branch-by-abstraction
through the existing admission decision and replacement action. DDIA score:
**10/10** because exact identity/version ownership, atomic install, bounded
predecessor state, fail-closed maintenance, and liveness fallback are explicit.

## Certificate Invariants

1. Only a successful pre-install successor service turn creates a
   certificate.
2. The certificate is keyed by exact stable identity and logical generation.
3. The recorded path generation is the service turn's exact ACK-owned path.
4. The proved floor is the positive cwnd sampled after the successful turn;
   it is not configurable and never ratchets during that generation.
5. A certificate is ready iff identity and path match and current
   `cwnd >= proved_floor`.
6. A path change or a current cwnd below the proved floor makes the
   certificate stale for new admission only; it cannot close or mutate the
   generation.
7. An initial or otherwise uncertified generation remains Unknown, preserving
   existing availability rather than manufacturing negative evidence.
8. A stale certified primary is not replaceable by this seam. A stale
   auxiliary may request exactly one existing generation replacement.
9. At most one global predecessor drains, and no same-open second replacement
   is introduced.
10. Replacement failure leaves the predecessor current and uses the existing
    attempt-local, qualified-only business fallback before any payload exists.

## Admission Ordering

For each observed candidate:

1. preserve existing PLPMTUD forward qualification;
2. classify successor service certificate as `Unknown`, `Ready`, or `Stale`;
3. exclude stale certified candidates while a non-stale candidate exists;
4. request existing auxiliary replacement for one stale auxiliary when its
   slot and global draining permit allow it;
5. then apply unchanged service-normalized load, raw ownership, path-service,
   and stable-index ordering to admitted candidates;
6. retain the existing bounded all-degraded/all-stale availability fallback.

Certificate staleness cannot re-admit a PLPMTUD-degraded candidate. Forward
qualification remains the first categorical safety policy.

## Capacity And Reachability Gate

The failed phase requested `18,926,124 bit/s` (`2,365,765.5B/s`) at about
`173ms` RTT. One 128KiB receiver interval needs `131,072B` within its complete
one-second window.

```text
stale start: 17,360 * (1 + 2 + 4) = 121,520B < 131,072B
proved start: 24,800 * (1 + 2 + 4) = 173,600B > 131,072B
```

The existing flight-ownership deterministic path starting at about 24KiB
already exceeds the `1,990,080B` final-growth bound for a 2MiB stream. The
certificate therefore fixes the newly selected admission precondition and is
intended to be sufficient for the next exact short-flow qualification. It is
not sufficient evidence for formal M2.

The unchanged exact 32MiB Endpoint gate must remain above `170 Mbit/s`,
complete exact EOF, report zero socket would-block, and finish at or below
`61,440/0/0B`.

Affected path:

```text
smoltcp Target open
  -> TuicUpstream::live_tcp_conn
  -> Quinn scalar observation + generation certificate
  -> TcpPoolAdmission certificate/qualification decision
  -> existing ReplaceAuxiliary action when stale
  -> handshake + exact successor service turn
  -> existing generation/activity CAS install
  -> existing Connect / D16 / Quinn / Endpoint / Exit / Target path
```

## Old-Path Audit

Exact transport-write flight ownership, successor authentication/service-turn
packet ownership, forward black-hole qualification, service-normalized
admission, replacement failure fallback, startup scheduling, writer ACK-stall
rebind, path reset, UDP-demand recovery, D16, Endpoint pacing, MTUD, Cubic,
GSO, and self-wake all remain active. No old local writer or egress path is
bypassed.

## Failure Discriminators

Selection/replacement logs must include certificate state, proved floor,
proved/current path generation, current cwnd, and the stale reason.

- stale certificate reaches one fresh replacement and the next qualification
  has no receiver-zero interval: retain the architecture;
- current service is at or above its floor despite later congestion events:
  do not replace; this proves the anti-churn branch;
- path or identity mismatch requests replacement: retain exact ownership;
- replacement is blocked/lost/timed out: preserve predecessor and bounded
  fallback without payload replay;
- a receiver-zero recurs after a fresh certified generation owns the exact
  flow: reject this architecture and do not tune or repeat unchanged;
- any D16, Endpoint, TUN, UDP, route, cleanup, CAS, or predecessor lifecycle
  regression rejects implementation.

## TDD And Stop Rule

The tracer RED replays the exact selected state: auxiliary successor
certificate floor `24,800B`, current cwnd `17,360B`, same identity/path, zero
black holes, and an available primary. The old policy reserves the auxiliary;
the required behavior requests one auxiliary replacement.

Subsequent vertical slices prove ready-at-floor anti-churn, path/identity
staleness, uncertified compatibility, replacement unavailability, all-stale
fallback, exact install/CAS certificate transfer, and replacement-failure
fallback. A realistic delayed Quinn pair with bounded injected ordinary loss
must preserve first-128KiB service after fresh qualification.

Expected RED may enter minimal GREEN. Any static threshold, event-count
blacklist, in-place service turn, new retry/timer, pool expansion, frozen-value
change, local capacity at or below `170 Mbit/s`, or unexpected regression stops
implementation. After all gates and code review pass, take exactly one paired
Mac qualification; do not run formal M2 first.
