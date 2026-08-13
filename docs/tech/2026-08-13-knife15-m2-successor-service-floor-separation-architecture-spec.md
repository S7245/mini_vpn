# Knife15 M2 Successor Service Floor Separation Architecture Spec

Date: 2026-08-13

Status: **LOCAL TDD IMPLEMENTED; COMPLETE GATES AND REVIEW IN PROGRESS; FORMAL
M2 AND M3 BLOCKED**

Failure artifact:
`/tmp/mini_vpn_knife15_macos_20260813_053346.tar.gz` (SHA-256
`6aeddcf104aca085f7935c7e2c64791f30fd9c6df2fa9a0c8d57fe702821f074`).

Paired Exit observer:
`/tmp/mini_vpn_knife15_exit_target_observer_20260813_053436.tar.gz`
(SHA-256
`8a0c0c1941f4f1033479fd9299d6e9fe27bc925f36a9767b2f6e2500eeedee38`).

## Decision

Deepen the existing TCP pool generation-replacement module by separating two
transport facts that currently share one `final_cwnd` value:

```text
readiness_certificate_floor = cwnd after the first exact ACK-owned service turn
install_handoff_proof       = cwnd after all turns needed to reach predecessor handoff
```

The immutable successor service certificate records the first value. It is the
minimum exact service that every freshly authenticated successor must prove,
independent of how large a predecessor happened to be at one replacement.

The second value exists only as proof for the current install transaction. The
generation-slot install CAS compares it with the exact predecessor-owned
handoff requirement, then discards it. The installed generation begins its
future replacement floor at its readiness certificate floor. If a later
replacement is actually authorized, the existing slot-owned handoff atomically
publishes:

```text
required_floor = max(readiness_certificate_floor, current_generation_cwnd)
```

The successor must again prove that exact current handoff before install.

No timeout, workload, rate, pool, cwnd threshold, or congestion-control value
changes.

## Selected Failure

The formal run passed nine complete cycles and 87 phases. Cycle 10
`short-reverse-4` never opened a Target connection: the iperf JSON contained no
connected stream or interval and was terminated by the unchanged 40-second
hard bound.

Immediately before the failed open, auxiliary generation 2 was idle and on the
same stable identity and path generation, with zero new black holes:

```text
install-time multi-round final proof: 3,605,919B
current native Quinn cwnd:               381,502B
path generation:                                0
black holes:                                    0
```

Admission classified `381,502 < 3,605,919` as `stale_cwnd` and started a fresh
generation replacement. Its ninth service turn lost one `1,409B` packet, so
the exact loss-fail-closed proof rejected the successor. The other pool lane
was not a qualified fallback and the business open returned `Saturated`; no
TUIC Connect or Target socket was created.

The paired Exit observer captured `57,705,606` packets with zero kernel drops.
Target counters stopped before the phase and never advanced for it, while TUIC
counters saw only the replacement proof. The Mac process, Endpoint, TUN,
physical path, Exit, Target, routes, DNS, and cleanup were healthy. This is a
pool-admission architecture failure, not an external-network or throughput-SLI
failure.

## Why Separation Is Necessary

Removing cwnd readiness entirely is rejected. A prior exact qualification
showed that a same-identity/same-path successor that contracted
`24,800 -> 17,360B` could miss the first 128KiB receiver interval. The one-turn
floor remains load-bearing.

Keeping the multi-round final cwnd as a permanent floor is also rejected. The
`3.6MB` value represented the predecessor's service at one historical handoff.
Native Cubic recovery may later reduce cwnd without changing identity, path, or
black-hole ownership. Treating every such reduction as destructive staleness
causes repeated replacement churn, consumes Endpoint capacity, and can turn one
ordinary proof-packet loss into a failed business open.

In-place service turns, static ratios, expiry timers, retrying a failed proof,
extra pool lanes, and relaxing qualified-only fallback are rejected. They add
new ownership or tuning while the existing fresh-successor and install-CAS seam
already contains the required safety transaction.

## Deep Module And Interface

`TcpPoolGenerationSlot` remains the deep module. Its interface continues to
offer replacement preparation and atomic install; callers do not own floor
arithmetic or generation state.

`TcpPoolSuccessorForwardServiceProof` supplies two scalar outputs to that
module:

- the first-turn readiness floor used to construct the immutable generation
  certificate;
- the final handoff proof used only by install validation.

`TuicUpstream` remains the outer Quinn adapter. `TcpPoolAdmission` consumes
only scalar identity/path/cwnd evidence and retains the existing Ready/Stale
policy. No new transport trait or Target probe is introduced.

The deletion test shows the separation earns its interface: without it,
readiness, historical handoff, current handoff, proof, and install ownership
again collapse into one number across admission and replacement callers.

Clean Architecture score: **10/10 target**. The inward policy remains
Quinn-independent. Refactoring score: **10/10 target** through Split Variable
and Introduce Parameter Object inside the existing branch-by-abstraction seam.
DDIA score: **10/10 target** because immutable generation evidence is separated
from transaction-scoped proof, with exact identity/version ownership and
atomic fail-closed install.

## Invariants

1. A successful first exact successor service turn produces one positive
   readiness floor on one exact path generation.
2. Additional turns may raise the transaction's final handoff proof but cannot
   raise the immutable readiness certificate floor.
3. Identity or path-generation mismatch remains stale and replaceable through
   the existing bounded seam.
4. Current cwnd below the first-turn floor remains `StaleCwnd`; current cwnd
   below only a historical multi-round final proof remains Ready.
5. The successor cannot install unless its final handoff proof reaches the
   latest exact predecessor-owned requirement.
6. Install does not persist the transaction-only final proof as the next
   generation's lifetime floor.
7. Future replacement preparation samples current cwnd under the exact slot
   mutex and publishes the maximum of that value and the readiness baseline.
8. Stale identity, zero cwnd, loss, path change, close, no progress, deadline,
   or insufficient install proof fails closed and leaves the predecessor
   current.
9. Existing streams stay on the drain-only predecessor; no Target request or
   business payload exists before install.
10. The configured two-slot pool, one global draining permit, qualified-only
    same-open failure fallback, and later-open replacement authority remain
    unchanged.

## Capacity And Reachability Gate

The failed short reverse rate was `42.239 Mbit/s`, but its local Target
control open needs only bounded request service. The exact current auxiliary
cwnd was `381,502B` at about `164ms`, roughly `18.61 Mbit/s` of one-window
send-side service; it was more than fifteen times the established one-turn
certificate floor and had unchanged identity/path/black-hole ownership.

The previous load-bearing short-forward discriminator still holds:

```text
17,360 * (1 + 2 + 4) = 121,520B < 131,072B
24,800 * (1 + 2 + 4) = 173,600B > 131,072B
```

Thus the first-turn floor retains the exact minimum-service purpose while the
transaction-only final proof continues to preserve a larger current handoff.
Endpoint application capacity must remain above the frozen `170 Mbit/s` local
architecture discriminator and terminate within
`available + live + outstanding <= 61,440B`.

End-to-end path:

```text
new Target open -> scalar pool admission -> optional replacement preparation
-> exact current handoff -> successor handshake -> first readiness turn
-> optional additional handoff turns -> generation/activity install CAS
-> TUIC Connect -> D16 -> Quinn -> Endpoint -> Exit -> Target
```

## Old-Path Audit

Successor certificate identity/path/cwnd admission, PLPMTUD forward
qualification, service-normalized selection, current-service handoff,
successor exact ACK/loss/path proof, install CAS, replacement failure fallback,
predecessor drain, reverse-gap ACK reinforcement, native Quinn congestion and
PLPMTUD recovery, Endpoint rebind/generation replacement, D16, TUN, Endpoint
pacing, MTUD, Cubic, GSO, and self-wake all remain active. The retired
mini_vpn `path_changed()` execution remains retired.

## TDD And Failure Discriminators

The first tracer replays a successful multi-round proof whose first turn is
about `26KB` and whose final inherited handoff proof is about `3.6MB`. The
installed generation must own a `26KB` readiness certificate and must not own
`3.6MB` as a future replacement baseline.

The next policy slice replays the artifact: same identity/path, zero black-hole
advance, first-turn floor below current `381,502B`, and historical final proof
above it. Admission must reserve the current generation without replacement.

Existing regressions must continue to prove:

- `17,360 < 24,800B` is stale;
- identity/path mismatch is stale;
- exact current `361,778B` handoff overrides an older `26,338B` baseline;
- install rejects a successor final proof below the latest handoff floor;
- proof loss remains terminal and business fallback remains bounded.

Any unexpected repair/regression failure stops for root-cause analysis. Local
32MiB throughput at or below `170 Mbit/s` rejects the architecture without
constant tuning. After focused/full/capacity/review gates pass, take one fresh
paired `m2-qualification`; do not run another formal M2 first.
