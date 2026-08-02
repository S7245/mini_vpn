# Knife15 M2 Path-Service-Aware TCP Pool Selection Architecture Spec

Date: 2026-08-02

Status: **LOCAL IMPLEMENTATION ACCEPTED — real M2 failed; fresh acceptance pending; M3 blocked**

## 1. Accepted Evidence

The exact HK bundle is:

```text
d4fc3bc6d47467e51864ff62fff82ba7a926ff322aa65e302decd9c771f1b31e
/tmp/mini_vpn_knife15_macos_20260801_053127.tar.gz
```

The run used start-owned source `6f1df4a`. Baseline, the 300-second direct
Target discriminator, start, smoke, M2 full-tunnel activation, system DNS,
public Exit, fake-IP HTTPS, the first complete M2 cycle, and the first three
phases of cycle 2 all passed. The direct Target discriminator delivered
`27.216 Mbit/s` with 300 complete receiver-positive intervals.

Cycle 2 `short-forward-1` then failed at `2026-08-01T06:00:14Z`:

- the Target receiver's complete `0.000000s -> 1.001225s` interval was `0B`;
- the client sent `19,922,944B`, but the Target received `9,961,472B`;
- the D16 data writer accepted `13,653,912B` and waited as long as
  `3,355,211us` for upstream write service;
- the Target stopped the data stream at the application boundary with
  `Stopped(0)`; D16 then closed at exact `queued=0 / leased=0 / reserved=0`;
- Endpoint conservation ended `61,404/0/0B`, below the frozen `61,440B`
  bound; TUN errors, local interface movement, resources, and route/DNS
  ownership remained clean.

The exact failure-window controls reject a general path outage:

- Exit probes at `05:59:37Z`, `06:00:08Z`, and `06:00:14Z` were `3/3`, `0%`
  loss, approximately `162ms` RTT;
- the physical gateway stayed `3/3`, `0%` loss and `en0` errors stayed at the
  lifetime baseline;
- conn0 continued authenticated QUIC RX and ACK progress;
- across the surrounding 30-second QUIC samples conn0 added only six lost
  packets and three congestion events.

The selection sequence is decisive. The iperf control stream selected conn1
at `active_before=60`. Once that relay's two half leases were visible, the
data open saw an equal nonzero pool load and stable-index tie-break selected
conn0 at `active_before=62`. Immediately before the failure:

```text
conn0: RTT=163ms, cwnd=10,124B, active data selection=62
conn1: RTT=163ms, cwnd=23,842B, active control selection=60
```

The failed conn0 data writer's `3.355s` wait was 4.4x the comparable first
cycle short-forward wait on conn1 (`0.761s`). The first-cycle short-forward
receiver started positive and completed; the failed stream did not.

Current `cebf30d` cleanup also worked as designed after macOS had reaped the
owned utun routes: the exact physical route state released all stale M2
markers, restored DNS, stopped the process, passed the secret scan, and wrote
an immutable cleanup-complete bundle. Cleanup is not part of this defect.

## 2. Diagnosis Tree

| Hypothesis | Prediction | Evidence | Verdict |
|---|---|---|---|
| Partial-tail observer false negative | only a final sub-second zero row | the zero row was the first complete 1.001225-second receiver interval | rejected |
| Exit/VPS or physical path outage | Exit/gateway loss or route/interface movement at failure | exact-window probes were clean and routes/interfaces were stable | rejected |
| TUN, D16, or Endpoint ownership failure | drops, stranded bytes, conservation excess, or local egress failure | zero TUN errors; D16 and Endpoint closed conservatively | rejected |
| QUIC connection death/black hole | no authenticated RX/ACK progress or open timeout | conn0 remained live and exchanged ACK/stream/datagram traffic | rejected |
| Equal-lease placement ignores current send service | equal active counts choose stable index even when path service differs | data chose 10,124B-cwnd conn0 over 23,842B-cwnd conn1 and then stalled 3.355s | selected |

This is the explicit stop branch from the accepted M0 lease-aware spec: the
corrected history-independent placement remained active, yet a short forward
still failed. Lease count is necessary lifecycle evidence, but one quiet
long-lived relay and one currently sending relay are not equal units of QUIC
service demand.

## 3. Goal

Deepen the existing `TcpPoolLeaseSelector` module so its small interface owns
both lifecycle load and current path-service tie-breaking:

1. active/opening lease count remains the first ordering key;
2. an entirely idle pool retains stable lowest-index ordering, preserving the
   accepted `conn0 -> conn1` control/data pair;
3. equal nonzero lease loads prefer the slot with greater current send-side
   QUIC path service, defined only as `cwnd / RTT`;
4. known service evidence outranks unknown evidence; equal or unknown service
   retains stable lowest-index ordering;
5. the existing preparation CAS and lease CAS remain the atomic authority;
6. selection diagnostics expose whether path service broke the tie and the
   selected plain-value sample.

The Quinn adapter samples volatile transport details; the selection policy
receives only a plain `cwnd + RTT` value. Tests exercise the same selector
interface used by production.

## 4. Non-Goals And Frozen Decisions

This stage does not change:

- TCP pool size `2`, pool connection construction, or primary ownership of
  UDP/heartbeat/health;
- H10d16/D16, Endpoint pacing, `30,720,000 wire B/s`, `61,440B` burst,
  `92,160B/1ms`, or `368,640B/10ms`;
- MTU, QUIC receive/send windows, chunking, Cubic, GSO default, self-wake, or
  rebind bounds;
- M2 rates, durations, receiver-positive SLI, UDP-loss SLI, real-client load,
  checkpoint counts, or route/DNS semantics;
- per-Target affinity, application priority, iperf special cases, retrying a
  failed TCP application stream, or moving a live stream between QUIC
  connections;
- the accepted cleanup repair.

Increasing the pool, tuning a threshold, changing congestion control, or
waiving the first receiver interval would hide the selected discriminator and
is forbidden.

## 5. Deepened Module And Interface

The existing pool selector is already the correct seam. Deleting it would
spread preparation ordering, CAS reservation, cancellation wake-up, and lease
accounting back into every open caller, so it passes the deletion test. The
right change is to deepen this module, not create another pass-through module.

The production flow becomes:

```text
TuicUpstream::live_tcp_conn
  -> try-lock each current pool slot
  -> adapt Quinn PathStats to plain TcpPoolPathService { cwnd, rtt }
  -> reserve_least_active(path_service[])
       minimum active lease load
       -> if minimum == 0: stable lowest index
       -> else greater known cwnd/RTT
       -> else stable lowest index
       -> preparation CAS
       -> active-count CAS
  -> selected slot mutex / existing probe or reconnect
  -> clone connection and release preparation guard
```

Service comparison uses exact cross multiplication in `u128`:

```text
left.cwnd / left.rtt > right.cwnd / right.rtt
iff
left.cwnd * right.rtt > right.cwnd * left.rtt
```

No rate threshold, smoothing constant, or configured weight is introduced.
Zero `cwnd`, zero RTT, a locked/replacing slot, or a missing sample is
`Unknown`. Unknown data never authorizes reconnect or close; it only loses an
equal-load tie to known current service and otherwise falls back to the stable
index.

The sample is intentionally point-in-time. It cannot make a volatile WAN
promise. Its leverage is narrower and testable: when lifecycle load cannot
distinguish two already-busy slots, do not deliberately place a new flow on
the lower current send-service path.

## 6. Concurrency, Safety, And Lifecycle Invariants

1. Every successful reservation increments exactly one observed slot load.
2. Every lease clone/drop preserves exact active ownership; underflow and
   saturation remain fail-closed.
3. Preparing slots remain unavailable, and a waiting selector still sleeps
   on `Notify` rather than spinning; it resamples path service after waking.
4. Path sampling never awaits a pool mutex and never holds more than one slot
   lock; it cannot invert the existing lock order.
5. A changing or stale path sample can affect only a tie-break. It cannot
   bypass least-load placement, preparation, probe/reconnect, or lease CAS.
6. An idle pool remains deterministic and independent of stale congestion
   history.
7. Primary UDP/health ownership and every existing reconnect generation rule
   remain unchanged.
8. A failed or cancelled open drops both reservation and preparation owners.
9. Diagnostics are emitted only under the existing TCP diagnostic gate; no
   payload-hot-path logging is added.

## 7. Capacity And Reachability Gate

The product's `100+ Mbit/s` target is at least `12.5 MB/s`. Frozen Endpoint
pacing retains approximately `239.167 Mbit/s` application capacity, so this
stage does not remove capacity from the byte-owned egress architecture.

At the failed decision point, the plain window/RTT estimates were:

```text
conn0 ~= 10,124B / 0.163s = 62,110 wire B/s
conn1 ~= 23,842B / 0.163s = 146,270 wire B/s
```

These are instantaneous service estimates, not achievable bandwidth claims.
They are nevertheless sufficient to order the two equal-load choices. The old
policy deterministically chose the lower estimate because its index was zero;
the new policy deterministically chooses the higher estimate.

The exact end-to-end hot path remains:

```text
macOS app -> owned IPv4 route -> utun/smoltcp
  -> open_tcp_relay -> live_tcp_conn -> deepened selector
  -> Quinn open_bi + TUIC Connect -> D16 ordered writer
  -> Quinn Endpoint pacing service -> UDP socket -> sing-box Exit -> Target
```

Continuous progress remains owned by smoltcp/TUN service, D16, Quinn stream
scheduling, Cubic, and the Endpoint pacing service. This change is intended to
be sufficient only for the observed equal-nonzero-load/lower-service placement
class. It does not claim arbitrary WAN loss can never produce a zero interval,
and only a fresh real M2 can accept the longitudinal result.

## 8. Old-Path Audit

Still active and unchanged:

- two persistent QUIC connections and the least-active first ordering key;
- preparation guards, per-slot mutexes, auxiliary stale probes, reconnect,
  authentication generation, and RAII lease halves;
- per-stream ACK-qualified Endpoint rebind for a continuous write stall;
- D16 byte ownership and Endpoint pre-accounting;
- primary TUIC UDP datagrams, full-tunnel real-client traffic, system DNS,
  fake-IP, and M2 cleanup.

Stable lowest-index tie-breaking remains reachable for an idle pool, unknown
service, and exactly equal service. It is removed only from the selected
equal-nonzero-load/unequal-known-service branch.

## 9. TDD And Failure Discriminators

Focused RED/GREEN tracer bullets:

1. equal nonzero loads plus `10,124B/163ms` versus `23,842B/163ms` must select
   slot 1; the current selector must fail this test RED;
2. an idle pool with the same unequal samples must still reserve slots `0,1`;
3. unequal lease loads must still choose the least-active slot even when its
   path service is lower;
4. known service must outrank unknown only for an equal nonzero load;
5. equal service and all-unknown samples must retain stable index ordering;
6. concurrent preparation, wake, cancellation, saturation, clone/drop, stale
   probe, and reconnect tests must stay green;
7. diagnostic formatting must expose the new policy, selected sample, and
   whether the service tie-break was used.

The next real M2 must show:

- no complete Target receiver-zero interval;
- equal nonzero load selections identify
  `policy=least_active_then_path_service` and the selected service evidence;
- no multi-second D16 initial writer service gap attributable to choosing a
  lower-service equal-load slot;
- Endpoint/D16/TUN/pool/resource/route/DNS/real-client/cleanup gates remain
  unchanged and pass;
- M3 stays blocked until full formal M2 plus stop cleanup is accepted.

If a fresh M2 fails with the new policy but selects the higher-service slot,
this hypothesis is rejected. Do not tune constants or repeat unchanged;
classify the exact stream ACK/service and same-window controls again.

## 10. Architecture Review

Current Clean Architecture score: **8/10**. The selection policy is local and
testable, but its interface currently sees only lifecycle counts and cannot
express the transport fact that caused the failure. Moving Quinn conversion
to a humble outer adapter and passing plain service values into the deepened
policy reaches **10/10 locally**; real M2 remains an acceptance fact, not an
architecture-score point.

Current DDIA/fault-tolerance score: **8/10**. Two QUIC partitions exist and
lease ownership is safe, but equal-count placement can create a service hot
spot. Path-service tie-breaking reaches **9/10 locally** while preserving
safety and bounded ownership. The remaining point requires real M2 evidence;
live-stream migration or a larger pool is deliberately not inferred from one
failure.

Deep-module score: **10/10 local**. One existing selector interface hides
load ordering, service ordering, atomic reservation, preparation, wake-up,
and stable fallback. The Quinn-specific adapter stays outside, while every
caller and test uses the same seam.
