# Knife15 M2 Service-Normalized Admission Architecture Spec

Date: 2026-08-07

Status: **APPROVED FOR LOCAL TDD IMPLEMENTATION; FORMAL M2 AND M3 REMAIN BLOCKED**

Failure evidence:
`docs/tech/2026-08-07-knife15-m2-cold-pool-placement-qualification-failure-results.md`.

## Decision

Deepen `TcpPoolAdmission` so an active ownership count is evaluated against
the current **TUIC TCP pool path service** that can serve it. Existing forward
qualification remains the first admission layer. After it determines which
busy candidates remain eligible, and only when every admitted candidate is
busy with known positive `cwnd/RTT`, select the candidate with the lower exact
service-normalized load:

```text
normalized_load = active_lease_ownership / (cwnd / RTT)
                = active_lease_ownership * RTT / cwnd
```

The comparison is an exact rational ordering, not a computed floating-point
score and not a bandwidth promise. Equal normalized load falls back to lower
raw lease ownership, then greater current path service, then stable index.
Idle candidates, unknown observations, and the explicit all-degraded fallback
retain the current least-active/equal-busy-path-service/stable ordering.

The policy changes only where a new Target stream is placed. It cannot close,
reconnect, replace, reset, retry, replay, migrate, duplicate, reprioritize, or
otherwise mutate a connection, stream, or payload.

## Compatibility With Prior Falsifiers

This does not reopen the rejected selector-score branch from the qualified
lane collapse or installed-successor failures. In those artifacts the
alternative generation was categorically degraded, or the already-selected
successor itself lacked initial service. This policy still excludes that
degraded generation before comparing load, and it does not replace Quinn
startup service. It would make the same choices in those historical states.

The new artifact exposes a different reachable state: both candidates remain
admitted, but a control reservation makes the warm path's raw ownership
larger than a roughly 73-times colder path. Exact demand/service ordering is
therefore a load invariant among eligible paths, not another health score,
threshold, or attempt to manufacture capacity.

## Goals

1. Prevent a control open from forcing its adjacent data open onto a path
   whose current service is orders of magnitude lower merely because its raw
   ownership count is smaller.
2. Treat lease ownership as demand only in relation to current path service,
   without adding a multiplier, threshold, hysteresis, timer, or knob.
3. Preserve forward qualification as categorical safety policy and generation
   replacement as the bounded escape path.
4. Preserve exact availability fallback when evidence is idle, unknown, or
   all degraded.
5. Keep all policy, CAS reservation, and focused verification local to the
   existing deep `TcpPoolAdmission` module.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, configured pool, QUIC windows, chunk,
  Cubic, GSO, Endpoint pacing, self-wake, recovery bounds, startup priority,
  workload, or SLIs.
- Do not introduce a `cwnd` ratio threshold, configured weight, target rate,
  or connection-affinity guess.
- Do not create traffic to warm a connection.
- Do not treat path service as health or let it re-admit a categorically
  degraded busy generation.
- Do not run formal M2 until local gates and one new bounded qualification
  pass.

## Deep Module And Dependency Direction

The inner `TcpPoolAdmission` module owns the pure normalized-load comparison
and reservation policy. Its interface consumes scalar
`TcpPoolPathObservation` values and returns one reservation decision. The
outer Quinn adapter supplies current cwnd, RTT, identity, and black-hole
counters; the inner policy contains no Quinn type.

The deletion test confirms the module is deep: deleting it would spread
qualification epochs, generation replacement, normalized ordering,
preparation ownership, active-count CAS, and fallback rules across both TCP
open callers. No separate comparator interface is added because it would have
one adapter and almost no implementation depth.

Clean Architecture score is **10/10** for this stage: dependency arrows point
from the Quinn adapter into scalar policy, and generic plus D16 open paths
consume one decision. DDIA safety/liveness remains explicit: categorical
qualification precedes load balancing, unknown evidence remains available,
and bounded fallback prevents incomplete observations from deadlocking new
opens.

## Admission Invariants

1. Forward qualification and auxiliary replacement are evaluated before
   normalized load.
2. A degraded busy candidate cannot regain admission through path service
   while any qualified/unknown alternative exists.
3. Normalized ordering is active only when all admitted candidates are busy
   and have known positive path service.
4. Idle or unknown evidence preserves current stable availability behavior.
5. All-degraded fallback remains byte-for-byte equivalent to current bounded
   least-active/equal-busy-service/stable ordering.
6. Exact normalized ties fall back deterministically; no randomness or
   floating point enters admission.
7. Preparation and active-count CAS remain the only reservation authority.
8. Draining predecessor ownership cannot penalize its installed successor;
   current-generation activity remains the admission load.

## Exact Ordering And Overflow Safety

For candidates `L` and `R`, lower normalized load is equivalent to:

```text
L.active * L.rtt / L.cwnd < R.active * R.rtt / R.cwnd
```

All source values are `u64`. The implementation must compare the positive
rationals exactly without an overflowing three-factor cross product. A small
private continued-fraction comparison inside `TcpPoolPathService` is allowed;
it remains an implementation detail of the deep module and must have max-value
tests. Saturation, floating point, lossy division, or a new dependency is not
allowed.

## Capacity And Reachability Gate

The qualification offered `13,026,762 bit/s` (`1,628,345B/s`). At placement:

```text
conn0: active=2, service=12,000B / 164.215ms  ~= 0.073MB/s
conn1: active=4, service=871,763B / 163.853ms ~= 5.32MB/s
```

Even after accounting for twice the raw ownership, conn1's normalized load is
about 36 times lower. The same generation had already carried tunnel smoke at
`28.645 Mbit/s` forward. Selecting conn1 is therefore plausibly sufficient for
the next exact `13.027 Mbit/s` qualification gate. It is not a claim about
`100+ Mbit/s` WAN acceptance.

The unchanged Endpoint gate must still exceed `170 Mbit/s` for 32 MiB with
exact EOF, zero socket would-block, and final conservation at `61,440/0/0B`.

End-to-end affected path:

```text
smoltcp Target open
  -> ProxyUpstream::open_tcp_relay
  -> TuicUpstream::live_tcp_conn
  -> acquire_tcp_pool_reservation
  -> sample_tcp_pool_observations (Quinn adapter)
  -> TcpPoolAdmission::try_decide (qualification, normalized load, CAS)
  -> TUIC Connect + D16 writer
  -> Quinn stream / Endpoint pacing
  -> sing-box mature server
  -> Target TCP
```

Every payload step after reservation is unchanged.

## Failure Discriminators

Selection logs must state whether service-normalized ordering was active and
retain exact selected plus candidate active/cwnd/RTT evidence. The next Mac
qualification is decisive:

- normalized admission selects the higher-service qualified generation and
  zero intervals do not recur: retain the architecture as a healthy
  differential comparator;
- it selects that generation but zero intervals recur with continuous writer
  ACK and Exit supply: reject normalized admission as insufficient and do not
  tune it;
- path evidence is idle/unknown so normalized ordering is not reached:
  classify the run as an admission-coverage mismatch, not proof of success;
- qualification, replacement, fallback, or CAS safety changes reject the
  implementation.

## TDD And Stop Rule

Focused RED must replay the exact two-open sequence. Starting from equal raw
ownership, the control open chooses the higher-service path. After its
reservation makes loads unequal, the data open must still select that path
because its service-normalized load is lower. Additional tests must prove
exact ratio ordering, max-value overflow safety, idle/unknown/all-degraded old
fallback, preparation CAS safety, and replacement current-generation
ownership.

Expected RED may enter minimal GREEN. Any frozen-value change, threshold or
weight knob, payload action, unbounded state, local capacity at or below
`170 Mbit/s`, or unexpected regression stops the implementation path. After
all local gates and code review pass, run exactly one bounded qualification
with the paired Exit observer; do not run formal M2 first.
