# Knife15 M2 Quinn New-Stream Startup Service Architecture Spec

Date: 2026-08-05

Status: **LOCALLY IMPLEMENTED AND REVIEWED — REAL M2 PENDING**

Evidence:
`docs/tech/2026-08-05-knife15-m2-installed-successor-startup-failure-results.md`

## Goal

Every new TUIC TCP relay must get bounded send service for its Connect header
and first non-empty business payload even when the selected QUIC connection
already owns old pending streams. After exactly one scheduling turn for the
first business payload, the stream must return atomically to its original
Quinn priority.

The caller-facing interface remains an ordinary `AsyncWrite`. TUIC owns the
policy “new Target stream needs startup service”; the vendored Quinn adapter
owns the atomic scheduler operation.

## Non-goals and frozen decisions

This stage does not change:

- connection admission, busy-epoch qualification, auxiliary replacement, or
  the configured/eligible pool of two;
- existing flow identity, replay, migration, reconnect, or reset behavior;
- D16/H10d16, MTU/PLPMTUD, QUIC windows, chunk, Cubic, GSO default,
  self-wake, recovery bounds, or TUN/smoltcp credit;
- Endpoint pacing `30,720,000 wire B/s`, `61,440B` burst, `92,160B/1ms`,
  `368,640B/10ms`, or its conservation equation;
- M2 schedule/rates, receiver-positive/TCP-gap/UDP-loss/resource/cleanup SLIs;
- target-specific rules, elapsed/cwnd/loss thresholds, or configuration knobs.

Priority is categorical and internal: original Quinn priority plus one for
startup, then the exact original value. It is not a configurable performance
parameter.

## Design tree

1. **Another pool score, cwnd floor, or repeat replacement — rejected.** The
   installed successor was selected exactly as designed. The stop rule closes
   this tuning class.
2. **Per-flow QUIC connections or a larger pool — rejected.** This changes the
   frozen pool and makes concurrency an unbounded connection-growth answer.
3. **Keep new streams permanently high priority — rejected.** A stalled new
   stream could starve unrelated established relays.
4. **Raise priority for a time/byte threshold — rejected.** It creates a new
   tuning constant and makes behavior RTT/rate dependent.
5. **One scheduler turn for Connect and first business payload — selected.**
   The queue event itself bounds authority and needs no timer or threshold.

## Deep module and seam

The TUIC startup-writer module has a small interface:

```text
arm(send stream)
write Connect header without consuming business startup authority
return ordinary AsyncWrite
```

Internally it:

1. records the stream's existing priority;
2. raises it by one before the Connect header is queued;
3. leaves the raised value armed while the stream has no business payload;
4. on the first successful non-empty business write, queues the bytes and
   restores the original priority under the same Quinn connection lock;
5. delegates every later write/flush/shutdown unchanged.

The production Quinn send stream and a deterministic fake writer are the two
adapters at this seam. Deleting the module would redistribute header/payload
phase knowledge, priority restoration, cancellation behavior, and diagnostics
across both TUIC open paths and every relay mode.

The vendored Quinn fork supplies one hidden atomic operation:

```text
write bytes with current queued priority
then set priority_after before releasing the connection lock
```

If the write is blocked, no bytes are accepted and the startup priority stays
armed. If the write succeeds, the already queued pending record retains the
startup priority for its next scheduler turn while any requeue observes the
restored priority. A concurrent driver cannot send multiple turns between a
write lock and a later restoration lock.

## Invariants

1. Connect bytes are eligible for startup priority.
2. An empty or blocked business write does not consume startup authority.
3. The first successful non-empty business write consumes it exactly once.
4. Queue insertion and priority restoration are atomic with respect to the
   Quinn connection driver.
5. At most the currently queued STREAM frame/packet turn retains startup
   priority; requeued payload has the original priority.
6. Existing streams, retransmissions, congestion/flow control, Endpoint
   reservations, and socket outcomes remain unchanged.
7. A stopped/closed stream cannot leave useful unrelated work starved; it has
   no new pending business data.
8. Both generic and native/D16 TUIC TCP open interfaces use the same module.
9. UDP datagrams and UDP stream relay mode do not use this priority contract.
10. No Target name, address, port, workload phase, rate, or elapsed time
    participates in the decision.

## Capacity and reachability gate

Cycle 8 offered `17,159,668 bit/s = 2,144,958.5B/s`. At the observed
`167.9ms` RTT the offered-rate BDP is about `360KB`; selected conn1 entered
with only `25,850B` cwnd and incumbent Apple/system/test streams. The local
sender nevertheless queued `2,228,224B` in its first second, while the Target
received zero, and the connection kept ACK/RX progress. The missing property
is first-service order, not source capacity.

Quinn's fair scheduler already orders pending streams by priority and then
round-robin recency. A one-level higher queued record therefore becomes the
first STREAM frame at the next congestion opportunity. Restoration before the
connection lock is released makes later frames rejoin ordinary fairness.

The service adds no queue and no extra payload. A startup frame remains
limited by one QUIC packet construction and the existing congestion window;
every resulting datagram is still pre-accounted by Endpoint pacing under:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

At MTU `1280`, the Endpoint burst can hold at most 48 wire datagrams and the
wire rate refills one MTU in about `41.7us`; the startup contract only chooses
which already-admitted stream occupies a connection send opportunity. It
cannot bypass Cubic, QUIC flow control, Endpoint DRR, or the socket.

This change is intended to be sufficient for the observed class: a
transport-live installed successor with old pending streams that delays a new
Target's initial delivery. It is not a guarantee against connection-wide ACK
loss or an external path outage; real M2 remains required.

## Hot path and old-path audit

```text
TUN/smoltcp async open
  -> TCP pool generation admission/replacement (unchanged)
  -> Quinn open_bi
  -> startup writer arms and writes TUIC Connect
  -> first D16 business poll_write queues one startup turn and restores
  -> Quinn priority/fairness -> Cubic/flow control
  -> EndpointPacingService -> UDP socket -> Upstream -> Target
```

All existing admission, D16, TUN, Endpoint, Cubic, retransmission, socket,
rebind, cleanup, and observer paths remain active. This is not a full
data-plane replacement; it replaces only the absent per-stream startup-service
contract.

## Observability and discriminators

When TCP diagnostics are enabled, emit one transition line per stream after
the first business bytes are accepted:

```text
tuic-tcp-startup-service ... stream=N first_payload_bytes=B
priority_before=P priority_after=Q state=consumed
```

Local tests prove queue order and exact one-shot restoration. Real acceptance
requires the failed-class Target receiver first complete interval to be
positive, with startup consumption visible and all existing controls/gates
passing.

If a fresh run consumes startup service but an installed successor again has
a healthy-control complete receiver-zero interval, reject this scheduler
contract as sufficient and open transport-level first-payload ACK service /
failover research. Do not add a second priority level, duration, byte budget,
or selector threshold.

## TDD and acceptance

1. RED/GREEN proto scheduler replay: an incumbent normal stream and a new
   armed stream; the new stream gets the next frame, then rejoins normal
   round-robin after atomic restoration.
2. RED/GREEN TUIC fake writer: Connect does not consume authority; Pending and
   empty writes do not consume it; first positive write restores once; later
   writes are ordinary.
3. Exercise generic, ordered, reassembly, native, and D16 construction paths.
4. Preserve endpoint recovery, generation replacement, connection selection,
   lifecycle, root/integration, runner, and vendored regressions.
5. Require release/Clippy/shell/fmt/diff/secret, exact 32MiB `>170 Mbit/s`, and
   code review with no unresolved P0/P1 before another real Mac run.

Design scores after the atomic Quinn seam: Clean Architecture **10/10**,
deep-module **10/10**, system design **10/10**, refactoring safety **10/10**,
and DDIA reliability/fault containment **10/10**.
