# Knife15 M2 Transport-Write-Demand Flight Ownership Architecture Spec

Date: 2026-08-10

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Failure evidence:
`docs/tech/2026-08-10-knife15-m2-transport-write-demand-flight-qualification-failure-results.md`.

## Stage Goal

Preserve congestion-growth ownership for the exact stream-offset prefix
accepted from a writer whose continuous demand was already proven by
`WriteError::Blocked`. Ownership must survive both Writable delivery and an
intervening Quinn connection-worker transmit turn before the writer retries.

The repair must not lend authority to another stream or to later bytes on the
same stream after the proven prefix is cumulatively acknowledged.

## Non-goals

- no D16, MTU policy/value, pool, QUIC window, chunk, Cubic, GSO default,
  Endpoint value, self-wake, retry, workload, SLI, or server change;
- no new queue, allocation, timer, wake, connection, stream, or replay;
- no reopening bounded sender, PacerCap64, GSO-only, or parameter tuning;
- no formal M2 or M3 claim from deterministic/local evidence.

## Paired Failure Boundary

Exact-source `b437b93` qualification passed baseline `25.429/54.539 Mbit/s`,
bounded direct `12.715 Mbit/s` without a complete zero interval, smoke, every
preflight, cycle 1, cycle 2 long forward/reverse TCP and reverse UDP, and
cleanup. Cycle 2 short forward then lost one complete Target receiver
interval. Formal M2 was not run.

The exact generation-3 stream accepted `6,296,563B` into Quinn and waited up
to `3,820,792us`. Paired Exit capture received about `4.23MB`, never paused
application supply for more than `165.430ms`, and had approximately `1..4ms`
Target ACK RTT with zero Target-side retransmit growth. The owning QUIC cwnd
started the phase at about `12,947B` and reached only `223,724B` by phase end.
This selects client-to-Exit congestion ownership rather than D16, TUN,
Endpoint capacity, Exit-to-Target, Target, cleanup, or a frozen value.

## Capacity Gate

The short phase requested `20,343,516 bit/s`, or `2,542,939.5B/s`. At the
observed `163..165ms` RTT, its bandwidth-delay product is approximately
`414,499..419,585B`.

The deterministic pair uses the production Endpoint policy, `82ms` one-way
latency, a `128KiB` send window, and a `2MiB` stream. The required growth
bound is:

```text
initial cwnd + total bytes - final send window
= 24,000 + 2,097,152 - 131,072
= 1,990,080B
```

The old implementation ends at `1,772,034B` when the worker packetizes after
write progress but before the next retry reaches Blocked. The intended path
meets or exceeds `1,990,080B`, which is sufficient at code level for the next
short-flow discriminator. WAN validation remains mandatory.

## Hot-path Inventory

```text
D16 run_relay_writer
  -> quinn::SendStream AsyncWrite
  -> quinn-proto SendStream::write_source
  -> Send offset / SendBuffer cumulative ACK prefix
  -> async Progress signal handoff
  -> Quinn connection worker / write_stream_frames
  -> exact STREAM offset packet ownership
  -> SentPacket ACK ownership
  -> Cubic::on_ack
  -> Endpoint reservation / UDP socket / Exit / Target
```

The existing writer, Quinn driver, Endpoint wake, ACK processing, and Cubic
controller continue to provide progress. No loop is replaced.

## Invariants

For every live send stream:

```text
write_waiting_for_retry == true
    only after the latest write returned Blocked and before nonempty progress

demand_through == Some(exclusive_offset)
    while an exact accepted prefix still owns proven transport demand

packet owns demand for STREAM [start, end)
    iff demand_through == Some(through) and start < through
```

On a nonempty retry after Blocked, extend `demand_through` to the stream's new
exclusive accepted offset and clear only `write_waiting_for_retry`. Do not
clear packet ownership merely because the application yielded after progress.

When the writer is no longer waiting and the first cumulatively unacknowledged
offset is at or beyond `demand_through`, clear the boundary. Reset,
STOP_SENDING, abandoned/error lifecycle, and 0-RTT rollback clear it
immediately. Finish clears the waiting edge but retains outstanding prefix
ownership until cumulative ACK.

Connection aggregation remains exact:

```text
transport_write_demand_streams
    == number of live streams whose demand_through is Some(_)
```

Only packets carrying an owning STREAM range override a later global
application-limited snapshot. Control packets, another stream, and later
same-stream offsets cannot borrow authority.

The Endpoint conservation invariant is unchanged:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

## Boundedness And Lifecycle

Each existing send stream adds one `Option<u64>` offset boundary alongside
the existing Blocked-retry bit. `StreamsState` retains one `usize` aggregate.
The packet hot path first rejects in O(1), then inspects only bounded STREAM
metadata already present in the packet. Cumulative ACK uses the existing
`SendBuffer` prefix; there is no per-frame state, scan, allocation, or queue.

## Necessary Versus Sufficient

This repairs a necessary ownership hole and is intended to be sufficient for
the next exact short-flow qualification because the deterministic path now
exceeds the observed WAN BDP by a wide margin. It is not sufficient for formal
M2: real loss, scheduler behavior, kernel/TUN, Exit forwarding, and cleanup
still require one paired Mac run.

## Old-path Audit

Unchanged paths include successor authentication/service-turn ownership,
ordinary Cubic and loss recovery, connection-local path reset, D16 queues and
ACK-stall rebind, pool selection, MTUD, QUIC windows, chunk size, Endpoint
`30,720,000B/s` and `61,440B` burst, GSO default, and self-wake. No old writer
or egress path is bypassed.

## TDD And Stop Rule

1. RED/GREEN the production interleaving: Blocked, Writable, successful
   partial retry, worker transmit turn, then the next Blocked retry.
2. Preserve the older Writable-before-retry growth regression.
3. Prove two streams keep independent ownership and a deferred writer cannot
   lend authority to another stream.
4. Prove cumulative ACK clears the exact boundary so later same-stream bytes
   do not inherit ownership.
5. Run complete Quinn/proto, Quinn adapter, root, integration, release,
   Clippy, shell, docs, formatting/diff/vendor/secret, and exact 32MiB gates.

If the paired qualification repeats a receiver-zero interval while the exact
offset-flight ownership is present, reject this architecture. Do not tune or
repeat unchanged.
