# Knife15 M2 Auxiliary Generation Replacement Architecture Spec

Date: 2026-08-04

Status: **APPROVED DIRECTION — implementation pending**

Evidence:
`docs/tech/2026-08-04-knife15-m2-qualified-collapse-failure-results.md`

## 1. Problem And Stop-Rule Result

Exact-source `fd6c34f` correctly isolated busy degraded conn1 and put both
failed short-forward opens on qualified conn0. Conn0 remained authenticated,
lossless at the physical controls, and free of PLPMTUD black-hole advances,
yet the Target's first complete receiver second was `0B`. The data writer
waited `1.169717s` after opening on a generation with `cwnd=5,140B`,
`RTT=175ms`, and fourteen existing lease halves.

The prior stage explicitly required this outcome to reject its placement
hypothesis. Adding another selector score, threshold, timer, or frozen-value
tune would ignore the falsifier. The missing capability is lifecycle: after
one logical slot is isolated, the pool cannot create a clean generation for
new work without killing the predecessor's unrelated streams.

## 2. Goal

Deepen the TUIC TCP pool into a generation-owning module with one caller
interface:

```text
acquire new Target relay
  -> reserve an eligible current generation, or
  -> prepare and install one auxiliary successor, then reserve it
  -> return connection + generation-bound lease + write-pressure adapter
```

When an auxiliary current generation is proven degraded inside a nonzero
ownership epoch, the module must:

1. authenticate one successor without closing the predecessor;
2. atomically make the successor the slot's only generation eligible for new
   opens;
3. bind the triggering relay to the successor;
4. keep every predecessor stream and its exact lease/write-pressure evidence
   alive until natural drain;
5. close and reap the predecessor only after its generation lease count
   reaches zero.

This is intended to restore the second clean admission lane that categorical
isolation removed. It is not a general WAN-outage guarantee.

## 3. Non-Goals And Frozen Decisions

The stage does not change:

- configured and eligible TCP pool size `2`;
- primary conn0, UDP datagrams, heartbeat, health, or failover-leg ownership;
- current flows, Target affinity, payload replay, stream migration, or retry;
- D16/H10d16, Endpoint pacing `30,720,000 wire B/s`, `61,440B` burst,
  `92,160B/1ms`, or `368,640B/10ms`;
- MTU/PLPMTUD policy, QUIC windows, chunk, Cubic, GSO default, self-wake, or
  endpoint-rebind bounds;
- M2 rates, duration, receiver-positive SLI, UDP `3%` SLI, or cleanup gates;
- bounded sender, PacerCap64, cap tuning, GSO-only, target-specific rules, or
  elapsed/cwnd/loss thresholds.

A predecessor and successor may coexist, but the predecessor is not an
admission slot. For the frozen pool of two, the hard lifecycle bound is:

```text
eligible generations <= 2
draining predecessors <= 1
live QUIC generations <= 3
```

That bounded overlap is generation replacement, not pool expansion.

## 4. Design Tree

### A. Add another placement score or exclude low cwnd

Rejected. The bundle already selected the qualified alternative. Any cwnd,
RTT, byte, or time floor is a new tuning branch and cannot create capacity.

### B. Prioritize initial bytes inside vendored Quinn

Deferred. It would change the multiplexing scheduler and every connection's
payload hot path without evidence that Quinn stream order, rather than the
single eligible generation's incumbent transport debt, is the sole cause.

### C. Close/reconnect degraded conn1 in place

Rejected. Conn1 still owned six lease halves. Closing it would tear down
unrelated established sessions and violate lifecycle correctness.

### D. Raise pool size or add a per-flow connection

Rejected. It changes the frozen pool and creates an unbounded scaling answer
to a bounded lifecycle defect.

### E. Auxiliary generation replacement

Selected. It uses the exact existing degraded discriminator, restores a clean
eligible lane, preserves all predecessor streams, and admits only one bounded
overlap. It is Branch by Abstraction plus Parallel Change: current generation
selection remains available while lifecycle ownership moves behind the new
module, then the old selector-only connection vectors are removed.

Structural score before implementation: **7/10**. Selection is deep, but
connection, generation, lease, pressure, and drain ownership are split across
parallel vectors in `TuicUpstream`. The target is **10/10** by applying
Introduce Parameter Object and Extract Class into one generation-owning
module; its deletion test would redistribute lifecycle races across open,
reconnect, endpoint recovery, diagnostics, and tests.

## 5. Deep Module And Seam

The module owns a fixed vector of logical slots. Each slot owns:

```text
current generation:
  Connection
  stable identity + reconnect generation
  generation-active lease counter + zero notification
  per-generation TcpWritePressure
  authentication/open state
  qualification anchor

optional draining predecessor (auxiliary only):
  Connection
  generation-active lease counter + zero notification
  per-generation TcpWritePressure
  drain reason and identity
```

The Quinn adapter supplies handshake/authenticate, stats sampling, and close.
A deterministic in-memory adapter exercises replacement/install/drain through
the same interface, making this a real seam rather than a test-only helper.

`TcpPoolSlotLease` becomes generation-bound. Clone/drop updates both:

```text
generation_active  // determines predecessor drain completion
pool_active_total  // preserves process-wide active ownership metrics
```

No caller may construct or swap counters directly.

## 6. Decision And Concurrency Invariants

1. Idle stable ordering remains conn0 then conn1.
2. Qualified candidates retain least-current-generation-active, equal-busy
   path-service, stable-index ordering.
3. A busy degraded primary is isolated but never generation-replaced here.
4. A busy degraded auxiliary with no draining predecessor requests exactly
   one replacement; the triggering reservation waits for and binds to the
   successor.
5. Replacement preparation is single-owner. Concurrent opens either reserve
   another eligible slot or wait/resample; they cannot install two successors.
6. Install uses expected predecessor identity/generation CAS. A stale
   handshake result cannot replace a newer connection.
7. Successor identity and black-hole anchor are committed only with the first
   successor reservation; observation alone never mutates authority.
8. The predecessor becomes permanently non-admitting before preparation is
   released.
9. Existing predecessor leases, streams, writer pressure, and diagnostics
   stay bound to the predecessor identity.
10. Generation active zero closes/reaps only that predecessor. It cannot
    close the successor or primary.
11. At most one predecessor drains. If the successor degrades before drain,
    diagnostics report `replacement_blocked=predecessor_draining` and the
    existing bounded fallback applies; no fourth connection appears.
12. Handshake/auth timeout or CAS loss leaves the predecessor/current state
    unchanged and returns an explicit open error. No payload has entered the
    uninstalled successor.
13. Primary connection reconnect and UDP semantics remain the existing
    single-source path.

## 7. Capacity And Reachability Gate

The short forward offer is `23,495,467 bit/s = 2,936,933 B/s`. At the observed
`175ms` RTT its rate BDP is about `514KB`. Conn0 entered the failed open with
only `5,140B` cwnd and incumbent streams. A fresh successor begins with a
smaller absolute window than the rate BDP, but has no incumbent stream debt;
its handshake/authentication completes before TUIC Connect reaches the
Upstream, and the first Target payload can receive service within successive
RTTs instead of waiting behind the isolated generation.

Frozen EndpointWindowV1 still provides about `239.167 Mbit/s` application
capacity. Three live generations share one endpoint-owned wire budget; they
do not multiply tokens:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

At equal three-way service the endpoint's application capacity remains about
`79.7 Mbit/s` per continuously active generation, above every M2 single-flow
rate. Replacement adds O(1) work at TCP open and no per-byte/per-packet work.

End-to-end hot path after install:

```text
app -> utun/smoltcp -> async Target open
  -> TCP pool acquire
     -> current observation / exact degraded discriminator
     -> optional auxiliary handshake + atomic generation install
  -> Quinn open_bi + TUIC Connect on successor
  -> D16 writer -> Quinn/Cubic -> Endpoint pacing -> UDP socket
  -> Upstream -> Target
```

Continuous progress remains owned by TUN/smoltcp, D16, Quinn, Cubic, and
Endpoint pacing. The new module owns only open-time lifecycle and drain.

Necessary versus sufficient:

- generation replacement is necessary because selector-only isolation has
  been experimentally falsified;
- it is intended to be sufficient for the exact class where a degraded busy
  auxiliary collapses new-open service onto one incumbent generation;
- it does not claim to solve the independent UDP `3.391937%` violation or an
  arbitrary physical outage;
- real M2 remains required.

## 8. Old-Path Audit

Unchanged and still active:

- two eligible pool slots and primary UDP/health ownership;
- TUIC Authenticate, open timeout, closed/idle liveness probe, and primary
  reconnect;
- D16 byte ownership, Endpoint pre-accounting, TUN/smoltcp service;
- per-stream ACK-qualified endpoint rebind;
- M2 full-tunnel, DNS/fake-IP, real-client, routes, resources, and cleanup.

Removed after Parallel Change completes:

- parallel `conns`, `tcp_open_states`, `tcp_startup_auth_attempts`,
  `tcp_write_pressures`, and admission-active vectors as independent
  lifecycle authorities;
- reconnect-in-place as the only possible auxiliary generation transition.

## 9. Observability

Add transition-only diagnostics:

```text
tuic-tcp-pool-generation-replacement-start
  slot predecessor_id/generation active black_holes

tuic-tcp-pool-generation-replacement-installed
  slot predecessor_id successor_id generation handshake_ms active

tuic-tcp-pool-generation-drained
  slot predecessor_id generation drain_ms

tuic-tcp-pool-generation-replacement-blocked
  slot reason=predecessor_draining
```

Selection lines add current/draining identities and whether a replacement
reservation installed the selected generation. QUIC/Endpoint stats must label
draining generations rather than attributing old writer pressure to the
successor.

Runner diagnostics also fail formal UDP phases immediately at loss `>3.0%`.
This changes no SLI; it prevents known-doomed long runs.

## 10. TDD

One vertical tracer at a time:

1. formal UDP `3.000001%` is RED as `phase complete`; GREEN makes it an exact
   `udp_loss_percent` phase failure while `3.0%` remains PASS;
2. replay `conn0 qualified active14 / conn1 degraded active6`; RED chooses
   conn0, GREEN returns auxiliary-replacement preparation;
3. install a fake successor and prove the triggering lease binds only to its
   identity;
4. prove predecessor leases keep their generation alive and zero closes only
   the predecessor;
5. prove concurrent preparation installs one successor;
6. prove stale install, handshake failure, primary degradation, and existing
   predecessor drain cannot grow or corrupt the pool;
7. prove endpoint recovery attributes writer pressure to both exact
   generations;
8. preserve every current admission, reconnect, UDP/health, D16, Endpoint,
   and runner regression.

## 11. Acceptance And Stop Rules

Local acceptance requires focused state-machine tests, root/integration,
release, Clippy, Knife15/Knife14 shell gates, vendored Quinn/proto, Endpoint
32MiB `>170 Mbit/s`, formatting, diff, secret, and code review with no P0/P1.

Then take exactly one fresh user-operated Mac transaction:

```text
m2-ipv6-check -> baseline -> direct-discriminator
-> start -> smoke -> m2 -> status -> stop
```

The run must show replacement start/install before the comparable short flow,
the Target receiver must have no complete zero interval, predecessor drain
must be exact or remain bounded/owned, and every frozen ownership/cleanup gate
must pass.

- Do not tune or repeat unchanged.
- A replacement that installs but still produces a healthy-control complete
  receiver-zero rejects this architecture and opens Quinn initial-stream
  service/failover research.
- UDP loss above `3%` fails immediately and remains an independent branch;
  do not change CC, MTU, payload, or SLI.
- Unexpected local regressions are diagnosed against the invariant before
  implementation continues.
- M3 remains blocked until full M2 plus cleanup acceptance.

Architecture review: Clean Architecture **9/10**, deep-module target **10/10**,
system-design **10/10**, refactoring safety **10/10**, TDD plan **10/10**.
