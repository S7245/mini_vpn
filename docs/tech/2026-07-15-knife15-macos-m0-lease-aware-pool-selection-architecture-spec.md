# Knife15 macOS M0 Lease-Aware TCP Pool Selection Architecture Spec

Date: 2026-07-15

Status: **ACCEPTED; LOCAL TDD IMPLEMENTATION PASS; FRESH M0 PENDING**

## 1. Problem Statement

The first formal M0 after a passing 300-second physical direct discriminator
completed sustained forward TCP, sustained reverse TCP, and reverse UDP. Its
first short forward TCP phase then failed the Target receiver continuity SLI.
The failure was not an operator, endpoint pacing, TUN, D16 ownership, resource,
or server-dial failure.

The decisive sequence was:

1. sustained forward opened control on connection 0 and data on connection 1;
2. sustained reverse opened control on connection 0 and data on connection 1;
3. reverse UDP opened one TCP control stream on connection 0 while UDP remained
   on the primary connection;
4. the global round-robin cursor was now odd;
5. short forward opened control on auxiliary connection 1 and data on primary
   connection 0.

The short data stream reached the exit immediately at the TUIC Connect layer,
but primary connection 0 began at minimum cwnd, accumulated loss/congestion,
and held the D16 writer for as long as 3.864 seconds. The Target receiver saw
two initial zero seconds and received only 3,670,016 of the client's
10,616,832 bytes before the 10-second phase closed.

Global opening parity is therefore an invalid pool-placement input. An
unrelated completed flow can invert the accepted control/data isolation of the
next iperf or application connection pair.

## 2. Goal

Replace global round-robin placement with a small, testable, lease-aware pool
selection module that:

- chooses the slot with the fewest active/opening lease units;
- resolves equal-load ties by the stable lowest index, so an idle pair starts
  with the primary connection;
- atomically reserves the observed slot load before returning the choice;
- exclusively marks that slot as preparing until its connection has been
  probed/reconnected and cloned, so a later opener cannot overtake an
  idle-exclusive preparation at the slot mutex;
- makes the immediately following concurrent/sequential open observe that
  reservation and choose the auxiliary slot;
- preserves the existing lease drop/clone accounting, slot mutex,
  stale-auxiliary liveness probe, reconnect, and primary UDP/health ownership.

For a two-slot idle pool the required observable sequence is:

```text
reserve control: [0, 0] -> conn0 -> [1, 0]
reserve data:    [1, 0] -> conn1 -> [1, 1]
```

After both leases are dropped, any unrelated one-flow phase may run and drop;
the next pair must again produce `conn0`, then `conn1`. No persistent cursor is
part of this decision.

## 3. Non-Goals And Frozen Decisions

This stage does not change:

- TCP pool size (`2`) or collapse all TCP onto primary/auxiliary;
- primary connection ownership of UDP, heartbeat, or health;
- H10d16/D16, MTU 1200, receive/send windows, chunking, Cubic, GSO, endpoint
  pacing constants, burst/window bounds, or self-wake;
- M0 rates, durations, Target receiver no-zero SLI, or M1 gate;
- stale timeout, heartbeat probe, reconnect thresholds, or QUIC congestion
  behavior;
- iperf flow semantics or target affinity.

A liveness probe is not a substitute for this change. Connection 0 was alive,
exchanging ACK/datagram traffic, and accepted a Connect immediately; its
problem was the quality of the selected data path during this short window.

## 4. Module And Interface

The deepened module remains local to `src/tuic.rs` because it owns the TUIC TCP
pool's lease counters and selection invariant. Its interface is one operation:

```text
reserve_least_active(slots) ->
    { index, lease, active_before, preparation_guard }
        | empty/busy/saturated error
```

The implementation scans current atomic loads for slots that are not already
being prepared, selects the first minimum, and uses compare-and-exchange to
claim the per-slot preparation flag and reserve exactly the observed load. If
another opener changed either state, it rescans. There is no global mutex held
while waiting for a per-slot QUIC mutex, liveness probe, or reconnect.

`live_tcp_conn` consumes that reservation before acquiring the selected slot's
mutex. The slot mutex remains the exclusive seam for cloning/reconnecting the
QUIC connection. A waiting reservation counts as load, so later opens can use
another slot instead of queueing blindly behind a slow probe or reconnect.
The preparation guard remains held through that mutex wait and connection
clone. Its RAII drop clears the flag and notifies one selector. If every usable
slot is temporarily preparing, the async selector waits on the notification;
it neither spins nor bypasses the ordering invariant.

The returned lease is the existing RAII owner. Every failure/timeout path drops
it and the preparation guard automatically; reader/writer clones continue to
keep a live relay counted. No caller can select a slot without reserving and
preparing it.

## 5. Concurrency And Lifecycle Invariants

1. Every successful selection increments exactly one slot before returning.
2. Every returned lease and its clones decrement exactly what they incremented.
3. Selection never underflows, wraps, or panics on an empty/saturated pool.
4. Stable ties prefer index 0; unequal loads prefer the least-loaded index.
5. Two simultaneous first reservations on an idle two-slot pool must occupy
   different slots.
6. A reservation made before a slot-mutex wait remains visible to later
   selectors.
7. A selector cannot enter a slot while an earlier selector is still preparing
   that slot; cancellation releases the gate and wakes a waiter.
8. Stale probing remains auxiliary-only and requires the selected reservation
   to have observed an idle slot.
9. Primary UDP/health lifecycle authority is unchanged.
10. Open failure invalidation and reconnect generation remain per-slot.
11. Removing the global cursor removes all history/parity influence from the
    active production path.

## 6. Capacity And Reachability Gate

### Target capacity math

The hard target remains `100+ Mbit/s` application throughput, or at least
12.5 MB/s. Endpoint pacing remains 30.72 MB/s wire rate with a 61,440-byte
burst and approximately 239.167 Mbit/s application capacity. This stage does
not reduce any queue, QUIC window, or pacing capacity.

One auxiliary data connection has already carried 179-185 Mbit/s in accepted
Knife14 evidence, including a run where the data connection carried 99.9998%
of transmitted bytes. Therefore selecting connection 1 for the first data
flow has a measured capacity path above both the 10.606476 Mbit/s M0 short-flow
offer and the 100 Mbit/s product gate.

Least-active placement also distributes later independent flows instead of
pinning them to that one connection. The scan/CAS cost is O(pool size), and the
frozen production pool has only two slots.

### End-to-end hot path

```text
TUN TCP establishment
  -> open_tcp_relay
  -> live_tcp_conn
  -> reserve_least_active (new selection seam)
  -> selected per-slot mutex / optional aux probe or reconnect
  -> Quinn open_bi + TUIC Connect
  -> D16 byte-owned reader/writer
  -> Quinn endpoint pacing service
  -> UDP socket / sing-box / Target
```

Continuous progress remains provided by the existing TUN/smoltcp loop, D16
writer, Quinn driver, endpoint pacing service, and its self-wake. This change
only chooses which already-provisioned QUIC connection owns a new TCP relay.

### Necessary versus sufficient

This is intended to be sufficient for the deterministic M0 phase-parity
failure class: after the one-control-flow UDP phase, the next short pair must
again isolate control on connection 0 and data on connection 1. It is not a
claim that arbitrary WAN loss can never violate a one-second continuity SLI.

### Old-path audit

The following remain active: two QUIC connections, primary UDP/health,
auxiliary stale probe/reconnect, per-slot open failure state, D16, endpoint
pacing, Cubic, GSO, and existing flow/window limits. The global round-robin
cursor and modulo selector must be removed from the reachable production path
and from tests that describe accepted placement behavior.

## 7. Failure Discriminators

With TCP diagnostics enabled, each selection line must expose
`policy=least_active` and `active_before=<n>` in addition to connection,
generation, probe, and reconnect evidence.

The next formal M0 discriminators are:

- short-flow control opens on connection 0 and data on connection 1;
- the Target receiver has zero zero-byte complete intervals;
- the data writer has no multi-second initial service gap;
- endpoint conservation stays at or below 61,440 bytes;
- D16 queue/lease/reservation ownership closes at zero;
- TUN, process-resource, and route/control gates remain clean.

If logs show the corrected placement but short forward still fails, this
change's hypothesis is rejected. Do not tune constants or rerun the same test;
reopen the flow-specific QUIC/path branch with a usable same-path mature-client
control.

## 8. TDD And Acceptance

Tracer bullets, one RED/GREEN cycle at a time:

1. an odd completed one-flow phase cannot shift the next idle pair away from
   `0, 1`;
2. simultaneous first reservations distribute across both idle slots and
   leave all counts at zero after drop;
3. a preparing slot cannot be overtaken before its connection clone;
4. an all-preparing waiter sleeps, wakes after RAII release, and then reserves;
5. empty and saturated pools fail closed without wrap or panic;
6. existing stale probe/reconnect and lease clone/drop tests remain green;
7. selection diagnostics identify the new policy and observed load.

Local acceptance passed: focused pool tests were `15/15`; root all-target tests
were `640 passed + 3 ignored` plus main `2/2`; release build, Clippy, Knife15
and Knife14 shell self-tests, formatting, and diff checks passed. Review found
and repaired the preparation-order P1 described above; no unresolved P0/P1
remains. The user alone runs macOS TUN M0. M1 stays blocked until a fresh M0
plus independent rearm pass.

## 9. Architecture Review Scores

- Clean architecture: **10/10 locally**. Policy is a small testable module;
  Quinn and mutex details remain outside its interface, and the production
  caller cannot bypass reservation/preparation.
- System design: **9/10**. Capacity, concurrency, failure modes, observability,
  and acceptance are explicit. The remaining point requires real macOS M0.
- DDIA/fault-tolerance: **9/10**. Reservation and preparation are atomic,
  bounded, reversible, wake-driven, and fail-closed; real scheduler/network
  behavior still requires acceptance.
- Refactoring structure: **10/10 locally**. This is branch-by-abstraction
  followed by removal of the old selector, not a big-bang pool rewrite; the
  obsolete cursor is gone and the full regression gate is green.
