# Knife15 M2 Busy-Epoch Forward Qualification Architecture Spec

Date: 2026-08-03

Status: **LOCAL COMPLETE — real M2 pending; M3 blocked**

## 1. Accepted Evidence

Two exact-source xiaoou bundles answer different questions.

### 1.1 Wi-Fi run: external path failure, useful comparator only

```text
53cfad0996cde38cff9d2577fbdcc80100d45a6a46639011b302b1093de221c1
/tmp/mini_vpn_knife15_macos_20260803_080212.tar.gz
```

Source `99e56b0`, reviewed runner `befb14c9...`, and release binary
`32774b82...` passed a fresh `13.522 Mbit/s` direct discriminator. M2 cycle 1
and its first short forward passed. The passing short flow used conn0 with
approximately `8,515B/173ms`, six congestion events, and zero PLPMTUD black
holes; the other slot already carried 35 black-hole detections.

Cycle 2 later failed a long forward phase after the Wi-Fi/Exit lane degraded:
14 complete receiver-zero seconds, conn0 loss/black-hole growth, Exit RTT up
to `829ms`, and gateway RTT up to `351ms`. That failure is external-path
evidence and does not authorize a product change. It does establish a useful
same-run comparator: a new short forward was placed on the slot with zero
black-hole debt and passed.

### 1.2 Ethernet run: product placement boundary

```text
804b96c5abaf40dd03040c3bf18d6176f7fb09d1c81b2962d73e462238b43f20
/tmp/mini_vpn_knife15_macos_20260803_093012.tar.gz
```

The same source/runner/binary ran on `en0 / Ethernet / 192.168.133.1`.
Baseline receivers passed at `34.527/60.550 Mbit/s`; the exact 300-second
direct Target discriminator passed at `17.264 Mbit/s` with zero sender and
receiver gaps. Start, smoke, IPv6, controlled full tunnel, public Exit, fake-IP
HTTPS, cycle-1 forward/reverse TCP, and reverse UDP (`2.259%` loss) passed.

Cycle 1 `short-forward-1` then failed:

```text
sender:   9,306,112B / 7.445 Mbit/s / 4 zero intervals
receiver: 3,801,088B / 2.987 Mbit/s / 2 complete zero intervals
```

The complete receiver-zero intervals were `0.000000–1.001046s` and
`2.000870–3.000857s`; this is not a final tail. During the exact failure
window the Ethernet gateway remained about `0.2–0.5ms` with zero loss and the
Exit remained about `158.5–160ms` with zero loss. Routes, interface counters,
TUN, D16, Endpoint conservation (`61,412/0/0B`, maximum `61,440B`), resources,
real-client preflight, and cleanup passed.

Immediately before the failed Target opens:

```text
conn0: cwnd=6,665B  rtt=176ms congestion_events=4   black_holes=0
conn1: cwnd=12,887B rtt=176ms congestion_events=362 black_holes=10
```

Control selected conn1 at `active_before=6`; data again selected conn1 at
`active_before=8`. Both choices were least-active decisions, so the current
path-service tie-break was not consulted. The data writer waited
`4,214,880us`; Target receiver service stopped twice for complete seconds.

The bundle contains the monotonic sequence from zero black holes on both
authenticated connections to conn1 reaching ten while its TCP ownership
remained nonzero. Conn0 remained at zero. The clean short comparator in the
Wi-Fi run also used a zero-black-hole conn0 while the other slot had deep
black-hole history. This selects busy-epoch qualification/isolation, not
another `cwnd/RTT` score or a parameter threshold.

## 2. Diagnosis Tree

| Hypothesis | Prediction | Evidence | Verdict |
|---|---|---|---|
| Operator/source/runner error | wrong commit/hash or skipped prerequisite | exact reviewed source/runner/binary; every prerequisite passed | rejected |
| Slow Ethernet bandwidth | average rate below a fixed floor | baseline/direct adapted the offered rate and had zero gaps | rejected |
| Same-window physical/Exit outage | gateway/Exit loss or RTT excursion during the zero intervals | both controls stayed lossless and stable | rejected |
| TUN, D16, Endpoint, route, or cleanup failure | drops, ownership excess, stranded bytes, or cleanup mismatch | all local conservation/lifecycle gates passed | rejected |
| Equal-load path-service bug remains | an equal busy tie chooses the lower `cwnd/RTT` slot | both opens chose a strictly less-loaded conn1; no tie existed | rejected |
| Greater instantaneous `cwnd/RTT` is sufficient | conn1's larger current sample prevents a complete receiver stall | conn1 had the larger sample and still produced two complete zero seconds | rejected |
| Busy slot with active-epoch PLPMTUD black-hole debt admits new flows | black-hole count advances while ownership stays live; a zero-debt slot exists but is bypassed by lease load | exact counter sequence and placement match; clean comparator used zero-debt slot | selected |

## 3. Goal

Deepen the existing TCP pool admission module so it owns one additional
categorical rule:

1. exact per-slot zero TCP leases start a new forward-qualification epoch and
   anchor the current monotonic Quinn black-hole count;
2. a busy slot is `qualified` while the count equals its anchor;
3. a busy slot is `degraded` after the count advances;
4. when the pool is busy and any non-degraded candidate exists, degraded
   candidates cannot receive a new Target relay;
5. the remaining candidates retain least-active ordering, equal-busy
   `cwnd/RTT`, and stable index;
6. if every candidate is degraded, the existing bounded ordering remains
   available and the decision is explicitly diagnosed as fallback.

This changes admission for only unopened TCP relays. Existing flows, primary
UDP/health ownership, and connection lifecycle remain untouched.

## 4. Non-Goals And Frozen Decisions

This stage does not change:

- pool size `2`, connection creation, live-stream placement, affinity, retry,
  reconnect, failover, or migration;
- H10d16/D16, Endpoint pacing, `30,720,000 wire B/s`, `61,440B` burst,
  `92,160B/1ms`, or `368,640B/10ms`;
- MTU/PLPMTUD policy, QUIC windows, chunking, Cubic, GSO default, self-wake,
  or endpoint-rebind bounds;
- M2 schedule, offered-rate math, receiver-positive SLI, UDP-loss SLI,
  direct/baseline gates, IPv6/full-tunnel ownership, or cleanup;
- PacerCap64, bounded sender, GSO-only, or any parameter-tuning branch.

Ordinary `congestion_events` do not become a hard gate: conn0 had four and
remained the clean comparator. Lifetime loss totals, loss ratios, EWMA,
weights, elapsed-time windows, byte floors, target names, and iperf knowledge
are also excluded.

## 5. Design-It-Twice Decision

Three interfaces were compared:

1. an exact-stream ACK-anchored adverse-event ledger minimized the surface,
   but the old bundle lacks its historical ACK anchor and small successful
   streams could requalify a slot before a bulk flow;
2. a generic opaque evidence-revision authority was extensible, but exposed a
   shallow source/fingerprint interface before a second real evidence source
   exists;
3. a caller-oriented admission module hid qualification, lease CAS, fallback,
   and diagnostics behind one reservation interface.

The third interface is chosen, narrowed to the exact discriminator present in
both bundles: PLPMTUD black-hole advancement within a nonzero lease epoch.
Active-zero is already the lifecycle seam that proves the prior ownership
epoch ended, so it supplies recovery without a timer, byte threshold, remote
probe, or writer-hot-path hook.

## 6. Deep Module And Interface

The current selector is renamed/deepened to `TcpPoolAdmission`. Its plain
input is independent of Quinn types:

```text
TcpPoolPathObservation
  Unknown
  Known {
    identity = stable_id + reconnect_generation,
    path_service = cwnd + RTT,
    black_holes_detected,
  }

reserve(observations[]) -> reservation {
  index, lease, preparation, active_before,
  selected qualification,
  qualification_override,
  all_degraded_fallback,
  candidate diagnostics,
}
```

The Quinn adapter remains in `TuicUpstream::live_tcp_conn`: it uses each slot's
existing `try_lock`, maps missing/locked/closed/invalid values to `Unknown`,
and never awaits a connection solely to obtain qualification. The admission
module has one short in-process per-slot state critical section, holds no Quinn
lock, never awaits while holding it, and retains the existing preparation and
active-count CAS as reservation authority.

Per slot the implementation hides:

```text
identity
black_hole_epoch_anchor

same identity + active=0    -> anchor=current, qualified
same identity + active>0:
  current == anchor         -> qualified
  current > anchor          -> degraded
  current < anchor          -> unknown/fail-closed observation
new identity + active=0     -> anchor=current, qualified
new identity + active>0     -> unknown until an exact idle epoch
missing/locked observation  -> unknown without state mutation
```

`Unknown` is not evidence of degradation. Only proven `degraded` candidates
are isolated. If any `qualified` or `unknown` candidate exists in an all-busy
pool, degraded candidates are removed; otherwise all-degraded fallback keeps
the product live.

If any candidate has `active_before=0`, idle least-active/stable ordering runs
first. This preserves the accepted startup `conn0 -> conn1` pair and naturally
opens a new qualification epoch.

## 7. Concurrency And Lifecycle Invariants

1. Every successful reservation increments exactly one active count and every
   clone/drop preserves exact lease ownership.
2. Preparation CAS still prevents two openers from preparing the same idle
   slot; an idle observation does not commit its epoch anchor until that slot
   wins the active-count reservation CAS, and waiters resample both path and
   qualification after wake-up.
3. Qualification state never authorizes reconnect, close, migration, retry,
   pool growth, or payload replay.
4. A stale identity cannot qualify a new connection while old leases remain.
5. Counter regression is `unknown`, never a reset or PASS.
6. A locked/closed/missing Quinn observation cannot mutate the epoch anchor.
7. A degraded active slot retains every existing stream and lease; only new
   Target admission changes.
8. All-degraded and all-unknown states remain bounded and nonblocking.
9. Diagnostics remain under the existing TCP diagnostic gate and do not add a
   payload-hot-path log.

## 8. Capacity And Reachability Gate

`100 Mbit/s` is `12.5 MB/s`. Frozen EndpointWindowV1 remains capable of about
`239.167 Mbit/s` application throughput. This stage performs only O(pool)
plain comparisons on TCP open, with pool fixed at two; it adds no per-packet,
per-byte, pacing, TUN, D16, or Quinn send work.

The exact hot path remains:

```text
app -> owned route -> utun/smoltcp -> async Target open
  -> live_tcp_conn -> TcpPoolAdmission
  -> Quinn open_bi + TUIC Connect -> D16 writer
  -> Quinn/Cubic -> Endpoint pacing -> UDP socket -> Upstream -> Target
```

Continuous progress remains owned by smoltcp/TUN service, D16, Quinn stream
scheduling, Cubic, and Endpoint pacing. Qualification changes only which
already-authenticated Transport slot receives a new stream.

This change is intended to be sufficient only for the observed class:
all slots busy, one slot has advanced its active-epoch black-hole count, a
non-degraded alternative exists, and least-active would otherwise select the
degraded slot. It is not a proof against arbitrary WAN interruption or a new
Mbps claim. Real M2 remains mandatory.

## 9. Old-Path Audit

Still active and unchanged:

- two persistent QUIC connections and per-slot lease ownership;
- preparation/active CAS, mutex preparation, stale auxiliary liveness probe,
  reconnect generations, and RAII lease halves;
- least-active ordering inside the admitted candidate set;
- equal nonzero `cwnd/RTT` tie-break and stable fallback;
- exact-stream ACK-qualified Endpoint rebind;
- D16 byte ownership, Endpoint pre-accounting, TUN/smoltcp service;
- TUIC UDP datagrams, real-client/full-tunnel M2, DNS/fake-IP, and cleanup.

The old unconditional least-active-first path remains reachable for idle,
all-degraded, and all-unknown states. It is removed only when a busy degraded
slot has a non-degraded alternative.

## 10. TDD And Diagnostics

Tracer bullets, one RED/GREEN at a time:

1. replay the Ethernet state: anchors `0/0`, active `8/6`, current black holes
   `0/10`, services `6,665/176ms` versus `12,887/176ms`; require conn0 and
   `qualification_override=true`;
2. prove active zero resets a slot's epoch and preserves idle `0 -> 1`;
3. prove an ordinary congestion-event difference alone cannot isolate;
4. prove all-degraded fallback keeps least-active/path-service/stable order;
5. prove missing, counter regression, and busy identity replacement are
   unknown and never mutate the anchor;
6. preserve concurrent preparation, wake/resample, cancellation, saturation,
   clone/drop, stale probe, reconnect, and prior path-service tests;
7. production adapter and formatting expose selected/candidate identity,
   active load, qualification, anchor/current black holes, path service,
   override, and fallback.

The next M2 must prove:

- no complete Target receiver-zero interval;
- the comparable short decision isolates a proven busy degraded slot when a
  non-degraded alternative exists;
- no D16 multi-second service gap attributable to admitting that degraded
  slot;
- Endpoint/D16/TUN/pool/resources/route/DNS/real-client/cleanup remain PASS;
- all-degraded fallback, if reached, is visible and cannot be relabeled PASS
  without reviewing the exact failure window.

## 11. Stop Rules

- Expected RED enters the smallest GREEN directly.
- Unexpected local regression is diagnosed and repaired in scope under the
  user's standing authorization; frozen values and SLOs cannot change.
- If local replay cannot choose conn0 without a threshold or Target-specific
  rule, reject this architecture.
- If fresh M2 correctly selects the qualified alternative and still has a
  healthy-control receiver-zero interval, reject this placement hypothesis;
  do not tune or repeat unchanged.
- If every slot is degraded and fallback correlates with failure, preserve it
  as the next connection-replacement/failover discriminator; do not expand
  pool size or reconnect active flows in this stage.
- M3 remains blocked until full M2 plus cleanup acceptance.

## 12. Architecture Review Scores

- Clean Architecture: **9/10 before implementation, 10/10 target**. Quinn is
  confined to an outer adapter and the plain admission policy owns all rules;
  the remaining point requires production migration without duplicated open
  paths.
- Deep-module: **10/10 target**. Deleting admission would spread qualification,
  preparation, lease CAS, fallback, and diagnostics across every opener.
- System-design: **10/10 design**. Requirements, exact capacity, hot path,
  failure model, observability, rollback, and non-goals are explicit.
- DDIA/fault tolerance: **9/10 local target**. Safety and availability fallback
  are explicit; the final point requires real M2 fault evidence.
- Pragmatic Programmer: **10/10 design**. One reversible tracer bullet uses an
  existing lifecycle fact and adds no speculative tuning surface.
