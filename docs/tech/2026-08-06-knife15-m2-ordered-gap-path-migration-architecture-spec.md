# Knife15 M2 Ordered-Gap Path Migration Architecture Spec

Date: 2026-08-06

Status: **REJECTED BY MAC QUALIFICATION; FORMAL M2 REMAINS BLOCKED.**

Superseded by:
`docs/tech/2026-08-07-knife15-m2-recovery-evidence-observer-architecture-spec.md`.

Source baseline: `0a71ebe4b1c147f9db5982ec3125cd54a495a067`.

Failure artifact:
`/tmp/mini_vpn_knife15_macos_20260806_104632.tar.gz` (SHA-256
`da12448ca74e56ed29c0b3603484eafff44d2f39d94a5fe2602337271422103b`).

## Decision

Expose one cloneable, read-only Quinn receive-stream progress handle and use
it to identify an exact ordered receive gap: the application has consumed a
contiguous prefix, the same stream has already buffered bytes strictly after
that prefix, but the missing prefix has not arrived. If the same gap survives
two consecutive existing `250ms` recovery samples, issue the existing
Endpoint UDP-socket rebind once for that exact gap episode.

The rebind starts QUIC active migration with a new local source port. It keeps
the QUIC identity, TUIC stream, Target TCP connection, and every application
byte. Quinn retains the previous socket while live pooled connections
authenticate on the new socket or drain. No stream is replayed or moved to a
different QUIC connection.

The trigger is deliberately narrower than TCP read `Pending`. TCP does not
tell a receiver whether a quiet peer intends to send more data, so elapsed
silence alone cannot distinguish a broken download from Apple Push,
long-poll, SSE, or a legitimate application pause. Buffered same-stream data
behind a missing ordered prefix proves both current remote supply and exact
head-of-line blockage without a Target rule or rate threshold.

## Failure Evidence

The exact-source run passed baseline at `46.328/59.947 Mbit/s`, the fresh
300-second direct discriminator, start/smoke, full-tunnel and real-client
preflights, cycle 1, cycle 2 forward, and cleanup. Cycle 2 reverse TCP failed
about 25 minutes into the formal schedule with eleven consecutive complete
receiver-zero intervals.

The reverse stream was conn1 stable id `43839895568`, stream 21. It eventually
received all `1,124,641,792B`, but one read gap lasted `12,630ms`. At the first
one-second pending observation, the connection had received another 111 UDP
datagrams, `96,732B`, and 62 STREAM frames since the last delivered stream
byte. The stream still could not return one ordered byte. Subsequent samples
showed no new datagrams or STREAM frames until recovery.

During the same window both pooled connections' local congestion windows
collapsed from roughly `1.5MB` to `2,818B`, while black-hole counters remained
unchanged. One Exit ICMP control packet was lost; the physical gateway,
routes, TUN, D16, Endpoint conservation, process, and interfaces remained
healthy. No connection-local path reset or Endpoint rebind occurred before
the failed transfer closed.

This establishes two facts:

1. The current writer-Pending plus new-PLPMTUD-black-hole predicate is not
   reachable for a downlink-only outage.
2. A client-only `Connection::path_changed` reset cannot reset the mature
   Exit's downlink congestion/path state. A source-port migration can, while
   preserving the existing QUIC stream.

The later sixteen generic rebinds happened only while evidence was retained
for about fourteen hours after formal M2 had already failed. They neither
caused nor repaired the selected transfer and are not acceptance evidence.

## Goals

1. Recover an established ordered TUIC TCP downlink whose missing QUIC stream
   prefix blocks already-received same-stream data.
2. Prove active receive demand structurally, without assuming that generic TCP
   silence is a fault.
3. Preserve the exact QUIC identity, TUIC stream, Target TCP, payload, pool,
   and D16 ownership.
4. Reuse the existing bounded Endpoint rebind and old-socket retention
   mechanism.
5. Consume recovery authority for one exact `(stable identity, reader,
   episode, read offset, next received offset)` before performing I/O.
6. Make receive-stream observation read-only and keep recovery policy outside
   vendored Quinn.
7. Replace another 24-hour attempt with a bounded two-cycle recovery
   qualification before formal M2 is eligible again.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool size, QUIC windows, application chunk,
  Cubic, GSO default, Endpoint pacing, self-wake, or existing recovery bounds.
- Do not trigger on ordinary read `Pending`, elapsed TCP silence, Target name,
  port, transfer rate, byte count, or application protocol.
- Do not replay, duplicate, reset, close, or cross-connection migrate a TUIC
  TCP stream.
- Do not add a new socket-migration timeout, configurable threshold, retry
  loop, or tuning branch.
- Do not describe the two-connection pool as path diversity while both
  connections still share the Endpoint socket.
- Do not rerun formal M2 until the bounded qualification and all cleanup gates
  pass.

## Considered Deepening Points

### Broaden generic TCP no-RX rebind — rejected, 3/10

It cannot distinguish legitimate quiet TCP from missing data and already
caused repeated Apple Push migrations. Demand-qualifying it by generic relay
ownership would repeat that defect.

### Reuse connection-local path-state reset — rejected, 5/10

It is correct for the earlier outbound writer failure, but it resets only the
client's send-side state and keeps the same source path. The selected failure
blocks the mature Exit's ordered downlink supply.

### Exact ordered-gap migration — selected, 9/10 before implementation

It adds one narrow observation seam at the framework boundary and reuses the
existing migration mechanism. Policy stays pure and the action preserves all
payload and lifecycle ownership.

### Cross-transport replay or multi-Exit failover — rejected, 2/10

TUIC v5 and the mature Exit expose no Target-byte acknowledgement contract
that can make replay of an established TCP relay safe. That would require a
new end-to-end resumable protocol, not a Knife15 recovery repair.

## Capacity And Reachability Gate

The failed reverse phase offered `29.973634 Mbit/s`, or about
`3,746,704 application B/s`. The accepted exact 32 MiB Endpoint gate reaches
more than `240 Mbit/s`; this stage adds no payload queue, copy, permit, or
pacing reservation and leaves the conservation law unchanged:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

At most three live pool generations and their live readers are sampled every
existing `250ms`. Each sample performs one bounded read-only stream-state
lookup. The read hot path receives no recovery lock and no additional payload
allocation.

Exact data and recovery reachability is:

```text
mature Exit Target read
  -> QUIC STREAM frames
  -> Quinn receive-stream assembler
  -> ordered prefix / buffered-tail progress snapshot
  -> TUIC per-generation read-pressure registry
  -> EndpointRecoveryState pure policy
  -> existing Endpoint UDP socket rebind
  -> Quinn local_address_changed + PING
  -> mature Exit observes new source path
  -> same QUIC stream retransmits missing prefix
  -> D16 -> smoltcp -> TUN -> local TCP receiver
```

The stage is intended to be sufficient for a missing-prefix outage that is
recoverable by QUIC active migration. It is not sufficient for a physical
outage common to both old and new source paths or for a mature Exit that does
not migrate the connection correctly.

## Quinn Observation Contract

`RecvStream::progress_handle()` returns a cloneable read-only handle. Sampling
it under the existing connection lock returns:

- `read_offset`: ordered prefix consumed by the application;
- `highest_received_offset`: greatest stream offset observed;
- `next_received_offset`: lowest buffered chunk offset, if any;
- `buffered_bytes`: bytes currently buffered by the assembler;
- `ordered_gap_bytes`: zero unless `next_received_offset > read_offset`.

The high-level handle contains no timer, recovery policy, Endpoint reference,
or mutation method. A closed/freed stream returns `ClosedStream`.

## Pure Policy Contract

- No buffered ordered gap means no read-pressure episode.
- First observation of a gap records its exact offsets and episode and emits
  no action.
- A second consecutive sampler observation is eligible only if stable id,
  reader, stream, read offset, and next received offset are unchanged and the
  ordered gap remains nonzero.
- Any ordered read progress, gap repair/change, stream close, or identity
  replacement clears that exact episode.
- An issued rebind covers the exact episode before the adapter call; failure
  cannot create an immediate retry loop.
- Existing hard writer ACK-stall Endpoint rebind retains precedence when both
  predicates become eligible in the same observation.
- Existing writer/black-hole connection-local reset remains unchanged and
  lower than hard writer ACK-stall; generic UDP-demand no-RX remains the final
  fallback.
- If several exact gaps qualify together, choose longest observed duration,
  then largest buffered tail, then stable identity/reader for deterministic
  bounded behavior.

## Deterministic TDD And Discriminators

Required RED/GREEN coverage:

1. Quinn/proto reports a nonzero ordered gap only when bytes exist beyond the
   missing prefix; contiguous data and empty/closed streams do not qualify.
2. One gap observation emits no action; the same gap on the next sampler turn
   emits one Endpoint rebind.
3. Read-offset progress, changed gap, stream close, or identity replacement
   emits no stale action.
4. A covered gap cannot rebind twice; a later gap at a greater read offset can
   own a new episode.
5. Quiet streams, first-byte waits, unordered readers, and readers with no
   buffered tail never qualify.
6. Writer ACK-stall precedence and connection-local writer recovery remain
   unchanged.
7. Vendored Quinn rebind preserves a live connection/stream and completes
   delivery through the new socket.
8. The exact 32 MiB Endpoint gate remains above `170 Mbit/s`, with exact EOF,
   zero socket would-block, and final conservation at `61,440/0/0B`.

The bounded Mac qualification must run exactly two formal workload cycles
after all ordinary preflights, then cleanup. It must report the trigger's
stable id, reader, stream, offsets, gap bytes, two-observation ownership,
socket generation, and current-socket recovery. The decisive outcomes are:

- action applied and no complete receiver-zero interval: retain the
  architecture and make formal M2 eligible;
- action applied but receiver-zero recurs: reject migration as insufficient;
- historical-style ordered gap occurs but action is not eligible: reject the
  observer/policy seam;
- no ordered gap and no receiver-zero: healthy comparator only, not proof that
  recovery works.

Do not tune or repeat an unchanged failed discriminator.

## Architecture Ratings

- Clean Architecture: **8/10 before implementation; target 10/10**. The pure
  policy and existing action boundary are correct; the missing read-only Quinn
  port and tested registry are the remaining work.
- DDIA fault tolerance: **8/10 before implementation; target 10/10**. Fault
  evidence is exact, action is bounded/idempotent, and old payload is retained;
  local and real-path liveness validation remain.
- Deep-module score: **9/10 before implementation; target 10/10**. A small
  progress API hides assembler details and keeps offsets, migration, D16, and
  runner policy from leaking into each other.
