# Knife15 M2 Recovery Evidence Observer Architecture Spec

Date: 2026-08-07

Status: **LOCAL IMPLEMENTATION COMPLETE; MAC QUALIFICATION REQUIRED; FORMAL M2
REMAINS BLOCKED.**

Failure evidence:
`docs/tech/2026-08-07-knife15-m2-ordered-gap-qualification-failure-results.md`.

## Decision

Remove exact ordered receive gaps from Endpoint rebind policy while preserving
the read-only Quinn receive-progress mechanism. Introduce one pure, bounded
recovery-evidence observer beside `EndpointRecoveryState`:

- an ordered-gap episode produces observation-only evidence after two
  consecutive existing sampler turns, including its initial and current tail
  state and whether the tail advanced;
- an exact writer-Pending episode produces a start record and one terminal
  aggregate containing ACK delta, ACK-progress samples, maximum Pending time,
  and maximum exact ACK-stall time;
- neither evidence type can rebind, reset, retry, replay, close, replace, or
  otherwise mutate a connection or stream.

The existing proven writer ACK-stall Endpoint rebind, writer plus PLPMTUD
connection-local path reset, and UDP-demand generic rebind remain unchanged.

## Goals

1. Restore safety by deleting the false-positive ordered-gap recovery
   authority without deleting useful Quinn evidence.
2. Preserve exact per-stream locality: evidence from unrelated streams or
   connection-wide counters cannot satisfy an episode.
3. Make the next short qualification distinguish ongoing ACK service from an
   exact writer ACK outage without adding a recovery guess.
4. Bound evidence to two records per observed writer episode and one persistent
   record per ordered-gap episode.
5. Keep all payload, queue, pacing, TUN, D16, pool, and transport behavior
   unchanged.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool, QUIC windows, chunk, Cubic, GSO,
  Endpoint pacing, self-wake, or recovery bounds.
- Do not replace the rejected `250ms` action with a longer timeout or a plateau
  threshold. The observer may report existing sampler facts but grants no
  authority.
- Do not replay, duplicate, reset, close, or cross-connection migrate payload.
- Do not infer Target delivery from QUIC ACKs. ACK evidence proves only
  client-to-Exit transport service.
- Do not run formal M2 until a new architecture is selected and qualified.

## Module And Seam

`RecoveryEvidenceObserver` is a deep module with one pure interface:

```text
observe(now, EndpointRecoveryInput) -> bounded evidence events
```

Its implementation owns episode anchors and aggregation. The existing
sampler adapter formats events to logs. Quinn remains an outer mechanism
adapter exposing read-only stream progress; `EndpointRecoveryState` remains
the mutation policy module. Deleting the observer removes diagnostics only,
whereas deleting recovery policy removes actions, so evidence and authority
have explicit locality.

## Writer Episode Contract

An episode key is `(stable identity, writer identity, episode identity)`.
On its first sampler appearance, emit `tcp_write_pressure_start` with stream,
Pending duration, exact ACK-stall duration, and acknowledged bytes. While the
same episode remains live, aggregate:

- observation count;
- first and final acknowledged bytes;
- count of samples in which acknowledged bytes advanced;
- maximum `pending_for`;
- maximum `ack_stalled_for`.

When the episode disappears or changes, emit exactly one
`tcp_write_pressure_end` record. A connection replacement or observer drop
cannot transfer an anchor to a new identity. No payload-path lock is added;
the observer consumes the existing sampler snapshot.

Interpretation is deliberately limited:

- ACK delta/progress greater than zero means the Exit continued acknowledging
  bytes during the sampled Pending episode;
- no ACK progress plus an exact ACK-stall at the existing recovery bound is
  already handled by the existing Endpoint policy;
- neither result proves whether the mature Exit supplied bytes to the Target.
  The paired Exit observer owns that discriminator.

## Ordered-Gap Observation Contract

The observer anchors exact `(stable identity, reader, stream, read offset,
next received offset)`. After two consecutive existing samples it emits one
record for the episode with initial/current highest received offset and
buffered bytes. Any exact prefix progress, next-offset change, close, or
identity replacement starts or clears an episode.

Tail growth is recorded explicitly. It is evidence that normal out-of-order
accumulation can coexist with the gap; tail plateau is only an observation and
still cannot authorize migration.

## Capacity And Reachability Gate

The failed short phase offered `19.221146 Mbit/s`, well below the accepted
Endpoint capacity. The observer samples already-materialized scalar snapshots
every existing `250ms`, holds at most one anchor per live reader/writer, and
adds no payload allocation or hot-path lock. The conservation law remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

The exact 32 MiB gate must remain above `170 Mbit/s` with exact EOF, zero
socket would-block, and final `61,440/0/0B`.

## TDD And Acceptance

Focused tests must prove:

1. a persistent ordered gap emits observation evidence but never an Endpoint
   action;
2. tail growth is preserved in the evidence;
3. progress, close, and identity replacement clear stale read anchors;
4. writer start emits once, ACK progress aggregates, and disappearance emits
   one terminal record;
5. writer episode or stable-identity replacement cannot merge evidence;
6. existing writer ACK-stall rebind and connection-local reset behavior remain
   unchanged;
7. the Quinn progress mechanisms and D16 registry lifecycle remain valid;
8. the exact Endpoint capacity/conservation gate remains green.

After local gates, run one bounded two-cycle qualification, not formal M2,
with the Exit Target observer active. Decisive outcomes are:

- no ordered-gap rebinds are possible by construction;
- a receiver-zero recurrence plus writer ACK progress and declining Exit
  application supply selects client-to-Exit per-stream service/scheduling;
- a receiver-zero recurrence with continuous Exit supply but broken
  Exit-to-Target writes selects the mature-server forwarding seam;
- no receiver-zero is a healthy differential comparator only.

## Architecture Scores

- Clean Architecture: **10/10 locally**. The Quinn mechanism seam is retained;
  evidence is physically separate from the mutation policy and is instantiated
  only when TCP diagnostics are enabled.
- DDIA fault tolerance: **10/10 locally**. The false timeout action is removed,
  events are identity-scoped and bounded, and the paired endpoint evidence is
  fail-closed pending Mac qualification.
- Deep-module score: **10/10 locally**. One small observer interface hides
  anchor lifecycle and aggregation from both recovery policy and log
  formatting.
