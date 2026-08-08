# Knife15 M2 Successor Authentication-Flight Ownership Architecture Spec

Date: 2026-08-08

Status: **LOCAL PASS; ONE PAIRED MAC QUALIFICATION PENDING; FORMAL M2 AND M3 REMAIN BLOCKED**

## Decision

Keep the existing five-second auxiliary-replacement deadline and one
current-cwnd successor service turn. At service-turn start, adopt every
ack-eliciting, congestion-accounted packet already in the current Data packet
space. These packets include TUIC authentication bytes that may have left the
client before the turn was created.

An adopted packet has exactly the same ACK, loss, path-generation, and
application-limited ownership as a packet created by the turn. A PLPMTUD probe
uses Quinn's special non-congestion loss path, but if the turn owns that probe
its loss must still publish the existing `PacketLost` terminal outcome.

## Failure Found By Code Review

`TuicUpstream::authenticate` opens a unidirectional stream, queues the TUIC
Authenticate command, and finishes the stream. TUIC has no application-level
authentication ACK. The replacement path then immediately starts the Quinn
service turn.

Before this repair, packets created before `start_successor_service_turn()`
were not tagged. A delayed authentication packet could therefore be outside
the exact success counters while later service packets were inside them. The
transport readiness gate did not own the bytes that establish the protocol
precondition for business traffic.

The review also found that Quinn intentionally removes a lost PLPMTUD probe
through a separate loss branch so it does not trigger congestion control. If
such a probe was adopted, the turn's `sent_bytes` included it but `lost_bytes`
did not, leaving the turn nonterminal until the outer deadline.

## Goals

1. Make successor readiness own authentication bytes already in flight.
2. Preserve the exact current-cwnd target and same-path ACK requirement.
3. Fail closed on loss of any owned ordinary or PLPMTUD packet.
4. Keep the repair bounded to existing Data-space sent-packet state.
5. Prove loss and success through deterministic real Quinn pairs before the
   next Mac qualification.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD policy, pool size, QUIC windows, chunk,
  Cubic, GSO default, Endpoint rate/burst/control reserve/quantum, self-wake,
  recovery, admission, workload, SLI, or runner timeouts.
- Do not add an authentication response, business replay, Target probe,
  second service turn, retry, connection, timer, configuration knob, or
  unbounded queue.
- Do not claim server application acceptance from a QUIC ACK. The invariant
  is transport delivery of the existing TUIC authentication bytes.
- Do not claim formal-M2 or WAN acceptance from local replay.

## Invariants

Let `C` be the snapshotted current congestion window and `A` the bytes in
eligible Data-space packets already in flight when the turn starts:

```text
target_bytes = C
initial sent_bytes = A
new carrier bytes >= max(0, C - A)
success iff sent_bytes >= C
           && acked_bytes == sent_bytes
           && lost_bytes == 0
           && path_generation is unchanged
```

Additional invariants:

1. Every adopted packet is tagged before any later ACK/loss is processed.
2. A path-generation mismatch, connection close, ordinary loss, or PLPMTUD
   probe loss publishes an existing terminal failure.
3. ACKed adopted bytes retain the service turn's non-application-limited
   congestion ownership.
4. No packet, payload, timer, or allocation is duplicated.
5. Endpoint conservation remains:

   ```text
   available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
   ```

## Capacity And Reachability Gate

The repair does not increase capacity. It closes a necessary ownership gap in
the already accepted current-cwnd readiness gate. For a fresh successor with
`C = 12,000B` and a small authentication packet `A`, the turn now owns `A` and
creates only the remaining current-cwnd carriers. All `sent_bytes` must be
ACKed before install.

Hot path:

```text
TUIC Authenticate queued and possibly transmitted
  -> Quinn Data-space sent packet remains in flight
  -> start successor service turn
  -> adopt the existing packet and exact byte count
  -> emit only the remaining current-cwnd carriers
  -> same-path ACK of every owned byte
  -> install successor generation

any owned loss/path change/close
  -> terminal failure
  -> preserve predecessor and use only the existing qualified fallback rule
```

This is a necessary correctness precondition for the next successor
qualification. It is not by itself sufficient to prove the first-128KiB
receiver interval or formal M2.

## Old-Path Audit

Endpoint pacing and its application-limited repair, service-normalized
admission, failed-replacement fallback, startup priority, writer ACK-stall
rebind, path-state reset, read-only recovery observer, UDP-demand recovery,
D16, TUN, and predecessor drain remain active and unchanged.

## TDD And Discriminators

Deterministic Quinn-pair tests cover:

1. authentication STREAM packet withheld before turn start: the turn adopts
   it and fails `PacketLost`, never succeeding past it;
2. authentication STREAM packet delivered: the peer receives the exact
   stream and the turn succeeds only after all owned bytes are ACKed;
3. an adopted PLPMTUD probe withheld: the special MTU loss path publishes
   `PacketLost` immediately rather than waiting for the outer deadline.

The next paired Mac run remains one qualification only. A repeated
installed-successor receiver-zero interval rejects this repair as sufficient
without tuning or repetition.

## Stop Rule

Stop on a new retry, larger/multiple service turn, protocol payload, Target
probe, frozen-value change, Endpoint conservation failure, exact 32MiB
capacity at or below `170 Mbit/s`, unexpected regression, or unresolved
P0/P1. Formal M2 and M3 remain blocked until qualification and cleanup pass.
