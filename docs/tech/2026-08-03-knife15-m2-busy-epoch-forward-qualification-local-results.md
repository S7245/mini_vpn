# Knife15 M2 Busy-Epoch Forward Qualification Local Results

Date: 2026-08-03

Status: **LOCAL COMPLETE — one fresh real M2 pending; M3 blocked**

Architecture:
`docs/tech/2026-08-03-knife15-m2-busy-epoch-forward-qualification-architecture-spec.md`

Plan:
`docs/tech/2026-08-03-knife15-m2-busy-epoch-forward-qualification-implementation-plan.md`

Implementation commit: `b40aa75`

## 1. Evidence And Rejected Prior Hypothesis

The exact-source xiaoou Ethernet artifact is:

```text
804b96c5abaf40dd03040c3bf18d6176f7fb09d1c81b2962d73e462238b43f20
/tmp/mini_vpn_knife15_macos_20260803_093012.tar.gz
source 99e56b0 / runner befb14c9... / binary 32774b82...
```

Baseline passed at `34.527/60.550 Mbit/s`, and the 300-second direct Target
discriminator passed at `17.264 Mbit/s` with zero sender/receiver gaps. Start,
smoke, IPv6/full-tunnel/real-client gates, cycle-1 long TCP, and reverse UDP
also passed.

The first ten-second `short-forward-1` then failed with two complete receiver
zero intervals. The physical gateway stayed at about `0.2–0.5ms` with zero
loss and the Exit stayed at about `158.5–160ms` with zero loss. TUN, D16,
Endpoint conservation (`61,412/0/0B`, maximum `61,440B`), routes, resources,
and cleanup passed.

Both Target opens selected conn1 because it was strictly less loaded
(`active_before=6`, then `8`), not because of an equal-load tie. Conn1 also had
the greater instantaneous service estimate (`12,887B/176ms` versus
`6,665B/176ms`) yet its data writer waited `4,214,880us`. This rejects the
previous claim that equal-load `cwnd/RTT` tie-breaking is sufficient.

The exact transport distinction was categorical: conn1 advanced from zero to
ten PLPMTUD black-hole detections while its TCP ownership remained nonzero;
conn0 stayed at zero. A useful short-flow comparator in the Wi-Fi artifact
`53cfad09...` also passed on the zero-black-hole slot while the other slot had
historical black-hole detections. No threshold, loss ratio, or target-specific
rule is needed.

## 2. Implemented Admission Module

`TcpPoolAdmission` now receives plain observations containing:

```text
stable connection id + reconnect generation
current cwnd + RTT
monotonic Quinn black_holes_detected
```

Each slot owns one hidden forward-qualification epoch:

- exact active ownership zero anchors the current identity and black-hole
  count as `qualified`;
- the same busy identity remains qualified while the count equals its anchor;
- an advance makes it `degraded` for new TCP opens until ownership reaches
  zero;
- missing/locked/closed evidence, counter regression, and a busy replacement
  identity are `unknown` without mutating the anchor;
- in an all-busy pool, proven degraded candidates are excluded only when a
  qualified or unknown alternative exists;
- if all candidates are degraded, the old bounded least-active, equal-busy
  path-service, stable-index order remains available and is diagnosed as
  `all_degraded_fallback=true`.

Idle `conn0 -> conn1`, preparation and active CAS, RAII lease ownership,
waiter resampling, stale probe/reconnect behavior, current flows, UDP/health,
D16, Endpoint pacing, and every frozen value remain unchanged. Quinn-specific
sampling stays in `live_tcp_conn`, uses `try_lock`, and adds no per-byte or
per-packet work.

Selection diagnostics preserve the existing prefix and lifecycle fields while
adding the selected qualification, anchor/current counters, isolation/fallback
flags, and both candidate summaries. Existing Knife14 parsing remains
additively compatible.

## 3. TDD And Review

The exact Ethernet tracer was RED because `TcpPoolAdmission`, transport
identity, path observation, and forward qualification did not exist. The
minimum GREEN anchors both idle slots, sets observed busy loads `8/6`, advances
only conn1 from zero to ten black holes, and selects the more-loaded qualified
conn0 despite conn1's greater instantaneous path service.

Focused coverage also proves:

- active-zero recovery and stable idle pairing;
- all-degraded least-active/path-service/stable fallback;
- missing evidence, counter regression, and busy identity replacement remain
  unknown and cannot reset an epoch;
- simultaneous open, preparation exclusion, saturation, clone/drop, and
  waiter wake/resampling invariants remain intact;
- an idle observation that has not won reservation authority cannot commit an
  epoch anchor or hide a concurrent busy-epoch counter advance;
- formatter output is complete and parser-compatible.

Review initially found that qualification sampling wrote an idle anchor before
the slot won reservation CAS. A deterministic RED reproduced the stale-sample
rewrite; the GREEN defers anchor commit until the successful active-count CAS,
while preparation still excludes a second opener. The stage review then found
no unresolved P0/P1. The only unrelated all-target gate
failure was the pre-existing short-run loop-profiler observer assertion. Two
RED runs showed the injected poll fraction rising strongly while aggregate
loop-active fell due relay/wall jitter. Commit `3ad7128` removed only that
contradicted monotonic assertion and retained the causal `poll +0.05`, range,
iteration, and completion gates; no production or M2 behavior changed.

## 4. Capacity And Local Gates

The admission decision is O(pool) only at TCP open with the frozen pool size
two. EndpointWindowV1 still exposes about `239.167 Mbit/s` application
capacity. Its explicit 32 MiB gate passed at:

```text
sender_mbps=240.154
available/live/outstanding=61,440/0/0B
socket_would_block_events=0
```

Final gates:

- focused TCP-pool tests: `27/27` PASS;
- root library: `678 passed / 3 ignored` PASS;
- root binary: `2/2` PASS;
- integration: `10 passed / 4 ignored` PASS;
- release build and all-target harness Clippy: PASS, established warnings only;
- Knife15 internal/external runner self-tests and Bash syntax: PASS;
- Knife14 low-RTT, US-client, and sing-box-control self-tests: PASS;
- vendored Quinn with exact local quinn-proto patch: `37 passed / 3 ignored`,
  doc `1/1` PASS;
- vendored quinn-proto: `309/309`, doc `3/3` PASS;
- root formatting and diff checks: PASS.

## 5. Acceptance And Stop Rule

This implementation is intended to be sufficient only for the observed class:
the pool is busy, one slot advances its active-epoch black-hole count, and a
non-degraded alternative exists. It is not a general WAN-outage guarantee and
does not accept M2 locally.

Take exactly one fresh HK transaction from the pushed reviewed source with a
rebuilt release binary:

```text
m2-ipv6-check -> baseline -> direct-discriminator
-> start -> smoke -> m2 -> status -> stop
```

Do not tune or repeat unchanged. If a qualified alternative is selected and a
healthy-control complete receiver-zero interval still occurs, reject this
placement hypothesis. If every slot is degraded and fallback correlates with
failure, preserve that discriminator for a later connection-replacement/
failover stage; do not expand the pool or reconnect active flows here. M3
remains blocked until complete M2 plus cleanup acceptance.
