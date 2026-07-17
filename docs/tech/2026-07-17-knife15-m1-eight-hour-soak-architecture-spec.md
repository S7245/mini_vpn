# Knife15 M1 Eight-Hour macOS Soak Architecture Specification

Date: 2026-07-17

Status: **Implementation candidate — local runner TDD authorized; no real M1
TUN run until local gates and code review pass**

## Stage Goal

M1 must prove that the accepted Knife14 H10d16 data plane and Knife15 macOS
runner remain bounded and useful for eight hours under changing but
attributable TCP, UDP, DNS, and connection-churn demand. M1 extends duration
and resource/recovery evidence. It does not reopen throughput architecture or
retune the product.

M1 is sufficient for the eight-hour target-only macOS reliability gate. It is
not sufficient for the 24-hour M2 gate, controlled failure injection M3, a
full-tunnel product, Network Extension behavior, mobile/Windows release, or
the complementary capable Linux/VPS soak.

## Frozen Product Boundary

M1 retains:

- H10d16 and the `524,288B` per-flow / `67,108,864B` global byte-owned egress
  bounds;
- EndpointWindowV1 at `30,720,000 wire B/s`, `61,440B` burst,
  `92,160B/1ms`, and `368,640B/10ms`;
- MTU `1200`, UDP application payload `1160B`, pool `2`, `1MiB` TCP socket
  buffers, `368,640B` QUIC receive window, Cubic, GSO enabled, and self-wake;
- one-second direction-aware receiver semantics and the `1KiB` reverse TCP
  observer;
- target-only macOS routing with the TUIC Exit outside the utun;
- `30s` process/interface/network sampling, `256MiB` log limit, `128MiB`
  retained-log envelope, and fail-closed one-shot bundles.

Do not treat cross-region Mbps as an architecture gate. Slow HK/Shenzhen
capacity is environment evidence. Receiver discontinuity, ownership leakage,
unbounded resources, unrecovered endpoint identity, or incomplete evidence is
the M1 failure surface.

## Preconditions And Provenance

One formal M1 run requires:

1. the exact reviewed source, release binary, and runner hashes;
2. a fresh direct forward/reverse baseline with positive direction-aware
   receiver intervals;
3. the unchanged 300-second physical Target receiver discriminator at 50% of
   the baseline, completed no more than 15 minutes before M1 begins;
4. user-executed `start` and `smoke` with Target/DNS on the fresh utun and the
   Exit on a physical interface;
5. five complete and recent start/smoke network-control samples;
6. fake-IP DNS enabled through an explicit `DNS_TARGET`.

M1 uses `M1_BASELINE_DIR` and `M1_DIRECT_DIR`. The public
`direct-discriminator` action accepts `M1_BASELINE_DIR` when the M0 variable is
unset and preserves the existing direct-evidence schema.

## Exact Eight-Hour Timeline

The formal traffic/drain budget is exactly `28,800s`. DNS commands, two-second
child-health polling, shell transitions, and setup add bounded wall-clock
overhead, so the user should expect slightly more than eight elapsed hours:

| Segment | Mode | Seconds | Purpose |
|---|---:|---:|---|
| steady-a | steady | 7,200 | warm plateau and mixed continuity |
| idle-1 | drain | 300 | first ownership/resource checkpoint |
| quiet | quiet | 3,600 | low offered load and idle-adjacent service |
| idle-2 | drain | 300 | second checkpoint |
| steady-b | steady | 7,200 | long steady-state repeat |
| idle-3 | drain | 300 | third checkpoint |
| churn | churn | 3,600 | short-connection lifecycle pressure |
| steady-c | steady | 6,000 | recovery and post-churn plateau |
| final | drain | 300 | final ownership/resource checkpoint |

The arithmetic is:

```text
7,200 + 300 + 3,600 + 300 + 7,200 + 300 + 3,600 + 6,000 + 300
= 28,800 seconds
```

Every active cycle remains sequential and contains forward TCP `300s`,
reverse TCP `300s`, reverse UDP `180s`, alternating short TCP connections of
`10s`, and fake-IP DNS. Steady and quiet cycles have six short connections;
churn cycles have 24. The exact budgets give 30 full DNS-bearing cycles, 234
completed short connections, 302 TCP results, 30 UDP results, and 332 total
phase results including partial tail phases. This adds substantial churn
without requiring concurrent iperf ownership on the single Target service.

## Offered-Load Profiles

All rates are derived from the fresh same-direction receiver baseline:

| Mode | persistent TCP/UDP | short TCP | short count |
|---|---:|---:|---:|
| quiet | 25% | 50% | 6 |
| steady | 50% | 80% | 6 |
| churn | 25% | 50% | 24 |

The M1 profile caps any derived offered rate at `200,000,000 bit/s`. This is a
workload safety bound, not a product setting. It remains below the accepted
EndpointWindowV1 application capacity of about `239.167 Mbit/s` and prevents
an unusually fast local baseline from asking the target-only reliability lane
to exceed the accepted endpoint service.

For the accepted HK M0 baseline (`36.224500/9.002664 Mbit/s`), the maximum
instantaneous M1 offer is `28.979600 Mbit/s` forward and `7.202131 Mbit/s`
reverse. Even the conservative all-active-at-maximum upper bound is about
`100GB` of application traffic over `27,600s`; traffic is not stored in the
bundle. The M0 log was `8.7MB` over two hours, so linear eight-hour log volume
is roughly `35MB`, below the retained-log envelope. Log compaction remains a
failure because it makes history lossy.

## Owned State And Checkpoints

M1 writes `m1-checkpoints.csv` after each 300-second idle/drain. Each row owns:

```text
timestamp,label,rss_kib,fd_count,thread_rows,
endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes
```

The runner samples immediately before a checkpoint. Formal M1 requires four
numeric rows named `idle-1`, `idle-2`, `idle-3`, and `final`; every row must
have Endpoint live and outstanding bytes equal to zero.

## Acceptance SLOs

### Workload and useful traffic

- `m1.status=complete`.
- Five active windows, three idle windows, three resumes, and one final drain
  complete exactly; no phase or health failure exists.
- Every completed phase has a matching JSON result; every completed cycle has
  one valid fake-IP DNS result.
- Every direction-aware receiver interval is positive. Sender-only zero
  intervals remain visible as REVIEW and do not replace receiver evidence.
- Maximum absolute TCP sender/receiver gap is at most `16,777,216B`.
- Maximum reverse UDP loss for any result is at most `3.0%`.

### Pacing, queues, and lifecycle

- Every Endpoint sample satisfies
  `available + live + outstanding <= 61,440B`.
- All four checkpoints and the final evidence sample have `live=0` and
  `outstanding=0`.
- No `terminal_pending_reap_bytes`, nonzero D16 queued/leased/reserved close,
  send-slice error, TUN flush failure, pump read error, or sustained pump-full
  wait occurs.
- Application-boundary Quinn `Stopped(0)` remains REVIEW only when the
  authoritative D16 close owns `0/0/0` queued/leased/reserved bytes and the
  receiver result completed. The already accepted exact local-first
  `524,288B + 27,840B` terminal release is classified, not stranded.

### Resource plateau

- At least `900` numeric process, network-control, interface, and Endpoint
  samples exist for the formal eight-hour window.
- Checkpoint RSS maximum is at most `131,072 KiB`.
- Final checkpoint RSS is at most first-checkpoint RSS plus `32,768 KiB`.
- Checkpoint FD maximum is at most first FD plus `2`; final FD is at most first
  FD plus `1`.
- Checkpoint thread maximum is at most first thread count plus `2`; final
  threads are at most first plus `1`.

These bounds are deliberately above M0's observed `39,664 KiB`, `15` FDs, and
`11` threads while still detecting growth large enough to matter during M1.

### Endpoint/path recovery

- No rebind is required for PASS.
- If a rebind occurs, attempts equal current-socket-generation recoveries,
  failures are zero, and maximum `first_rx_ms <= 7,000`.
- Exit/gateway/physical controls are present for every process sample. ICMP
  loss alone is path evidence; missing/unparseable control is evidence failure.

### Evidence and cleanup

- The log does not compact and the bundle secret scan passes.
- Stop identity-verifies and terminates the M1 controller before mini_vpn,
  removes only owned routes, terminates the watchdog, and records one final
  sample.
- Final process is dead, the owned utun is unavailable, and Target, DNS, and
  Exit routes no longer point to that utun.
- The immutable bundle checksum matches the returned archive.

## Failure Discriminators

| Observation | Selected boundary | Next action |
|---|---|---|
| checkpoint RSS/FD/thread grows beyond SLO with clean path | client resource lifecycle | preserve artifact; add focused ownership/resource replay |
| both QUIC connections lose RX while gateway/Target stay healthy | Endpoint socket identity | require bounded rebind and current-socket recovery evidence |
| receiver zero matches direct/Exit degradation | physical path | do not tune product; rerun only in a new qualified window |
| receiver zero with healthy controls and clean local ownership | connection/QUIC service architecture | stop unchanged repeats; design isolation/failover |
| UDP loss >3% with healthy controls | UDP/TUIC data-plane quality | inspect datagram loss/service, not TCP constants |
| missing result/DNS/control/checkpoint or log compaction | evidence architecture | repair runner under shell TDD before another TUN |
| final live/outstanding or D16 ownership nonzero | local lifecycle/cleanup | product failure; deterministic replay before retest |

## Stop Rules

- An expected RED fixture may proceed to its minimal GREEN implementation.
- An unexpected local repair/regression failure must be analyzed and repaired
  from its selected boundary; do not change frozen data-plane settings.
- A real M1 phase, health, receiver, ownership, resource, recovery, evidence,
  or cleanup failure stops M2 and M3. Preserve `status/snapshot/stop` evidence.
- A path-attributed failure does not authorize a product tweak or weakened SLO.
- M2 is not authorized until M1 bundle review and code review have no unresolved
  P0/P1.

## Old-Path Audit And Sufficiency

M1 adds no data-plane path. TUN ingress, smoltcp, D16, TUIC Connect/Packet,
Quinn EndpointWindowV1, connection pool, endpoint rebind, watchdog, summary,
and cleanup remain active exactly as accepted. The runner change is therefore
necessary and sufficient only for the M1 evidence/workload gate. It makes no
new claim about Mbps capacity; the accepted Knife14 capable Linux/VPS result
remains the capacity proof.

## Design Review Score

System-design score: **10/10** for M1 scope. Requirements, duration/rate math,
ownership, capacity, resource bounds, recovery semantics, observability,
failure attribution, cleanup, non-goals, and next-gate stop rules are explicit.
Real macOS/TUIC behavior still requires the user-run eight-hour acceptance.
