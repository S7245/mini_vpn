# Knife15 M2 Transport-Write-Demand Ownership Architecture Spec

Date: 2026-08-08

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Failure evidence:
`docs/tech/2026-08-08-knife15-m2-transport-write-demand-qualification-failure-results.md`.

## Stage Goal

Preserve congestion-growth ownership from the moment a QUIC send stream proves
continuous application demand by returning `WriteError::Blocked` until that
same stream either makes write progress or becomes terminal.

Separately, keep a preexisting PLPMTUD probe outside successor protocol
readiness while continuing to adopt and fail closed on preexisting
authentication STREAM packets.

## Non-goals

- no rate, burst, MTU, PLPMTUD policy, pool, QUIC window, TCP buffer, chunk,
  Cubic, GSO, Endpoint, D16, recovery-timer, or SLI tuning;
- no retry, second replacement, new connection, payload, protocol, or server
  change;
- no generic promise that every lossy WAN short flow is gap-free;
- no formal M2 or M3 authorization from local evidence alone.

## Capacity Gate

The failed short phase requested `14,016,912 bit/s`, or approximately
`1,752,114B/s`. At the observed `165ms` RTT its bandwidth-delay product is
approximately `289,099B`.

The installed successor began at `24,886B`. Loss-free slow start needs about
four RTT growth rounds (`24,886 * 2^4 > 289,099`), approximately `660ms`, to
reach the requested BDP. The old run instead needed the full ten seconds to
reach `271,543B` and confirmed only about `3.85MiB`.

The deterministic old path begins at `24,000B`, repeatedly proves transport
Blocked under `82ms` one-way latency, transfers `2MiB`, and ends at only
`151,200B`. The intended path ends at or above initial cwnd plus every
transport-demand-owned byte except the final application-complete send window.
This is a sufficient code path for the next short-flow gate, subject to WAN
loss validation.

## Hot-path Inventory

```text
D16 writer
  -> quinn::SendStream AsyncWrite retry
  -> quinn-proto SendStream::write / write_source
  -> StreamsState write/send-window ownership
  -> Connection::poll_transmit
  -> SentFrames / SentPacket per-packet demand ownership
  -> Connection::on_packet_acked
  -> Cubic::on_ack
  -> cwnd growth
  -> Endpoint reservation / UDP socket / Exit / Target
```

Continuous progress remains provided by the existing D16 writer, Quinn
connection worker, Endpoint wake, ACK handling, and Cubic controller. No new
loop or timer is introduced.

## Invariants

For each send stream:

```text
transport_write_demand == true
    iff the latest nonterminal application write reached transport Blocked
       and no later nonempty write made progress
```

Connection aggregation:

```text
transport_write_demand_count
    == number of live send streams with transport_write_demand
```

Packet ownership:

```text
packet.transport_write_demand == true
    iff the packet contains a STREAM frame for a stream whose
        transport_write_demand was true when the frame was emitted

Cubic ACK is application-limited only when:
  connection.app_limited
  and not packet.successor_service_turn
  and not packet.transport_write_demand
```

Ownership is cleared on a nonempty successful retry, finish, reset,
STOP_SENDING, or 0-RTT rollback. Writable event delivery alone does not clear
it. Multiple streams retain independent ownership. A cancelled or deferred
blocked writer cannot lend ownership to packets carrying another stream or
control traffic.

Successor pre-start adoption excludes only the exact active PLPMTUD probe
packet number. ACK-eliciting congestion-accounted authentication packets remain
adopted. Newly emitted service-turn packets retain existing exact ACK/loss,
path-generation, close, and deadline behavior.

## Boundedness And Lifecycle

Each existing `Send` gains one boolean. `StreamsState` gains one `usize`
aggregate. Each already tracked `SentFrames`/`SentPacket` gains one boolean.
There is no per-event allocation, queue, wake, retry, or timer. The transmit
hot path first rejects in O(1) when the aggregate is zero; otherwise it checks
only the bounded STREAM metadata already emitted into that packet (normally
one inline element). The count is reset with 0-RTT stream rollback and
decremented by every stream terminal path that can follow a blocked write.

The existing Endpoint conservation invariant is unchanged:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

## Necessary Versus Sufficient

This fixes a necessary transport-ownership defect and is intended to be
sufficient for the next exact short-flow qualification because the deterministic
path now has ample cwnd capacity for the observed BDP. It is not sufficient for
formal M2: real QUIC loss, scheduler behavior, TUN, kernel, and Exit forwarding
still require one paired Mac run.

## Old-path Audit

The following remain active and unchanged:

- successor current-cwnd service turn and exact tagged ACK/loss ownership;
- authentication-flight adoption and failed-slot business fallback;
- startup stream priority turn;
- Endpoint Bulk reservation ownership;
- Endpoint `30,720,000B/s`, `61,440B` burst, `92,160B/1ms`, and
  `368,640B/10ms` bounds;
- D16 queues and writer ACK-stall recovery;
- connection-local MTUD/path reset and ordinary Cubic;
- pool selection, windows, chunk size, GSO default, and self-wake policy.

No old throttling or writer path is bypassed.

## Failure Discriminators

The next run must report together:

- exact source and service-turn outcome;
- installed generation/cwnd/RTT;
- D16 writer accepted bytes, Pending maximum, and ACK progress;
- QUIC cwnd, loss, congestion events, PLPMTUD probes, and black holes;
- sender and receiver one-second intervals plus Target bytes;
- Endpoint conservation and TUN/interface errors;
- paired Exit supply/ACK capture when available;
- complete route/DNS/TUN/process cleanup.

If writer ACKs grow rapidly and Target still pauses, select Exit/Target. If
writer remains Pending with slow ACKs and cwnd again grows only linearly,
reject this ownership repair rather than tuning. If the service turn fails
only on authentication/ordinary packet loss, retain fail-closed behavior. An
MTU-probe-only loss must no longer reject it.

## TDD And Stop Rule

1. RED: continuous blocked writer under production Endpoint policy and
   realistic RTT ends `24,000 -> 151,200B` after `2MiB`.
2. GREEN: packets carrying demand-owned STREAM rounds grow cwnd; only the final
   application-complete window may be app-limited.
3. Prove per-stream ownership survives another stream's reset.
4. RED/GREEN: cancel/defer one blocked writer after Writable and prove an
   independent app-limited stream cannot borrow its cwnd authority.
5. RED/GREEN: an old PLPMTUD probe loss is recorded by MTUD but does not fail
   the service turn; old authentication loss still fails.
6. Run full root, integration, release, Clippy, shell, vendored Quinn/proto,
   docs, formatting, vendor, secret, review, and 32MiB capacity gates.

If the deterministic path cannot reach the stated growth bound, stop before
Mac. If the next exact paired Mac qualification repeats the same receiver-zero
with this ownership demonstrably active, reject the architecture and do not
repeat or tune unchanged.
