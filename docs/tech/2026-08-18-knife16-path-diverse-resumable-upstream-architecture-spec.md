# Knife16 Path-Diverse Resumable Upstream Architecture Specification

Date: 2026-08-18

Status: **ACCEPTED DIRECTION; IMPLEMENTATION NOT STARTED; M3 BLOCKED**

## Goal

Preserve established TCP delivery and bounded real-time UDP service through a
single-path transport stall without changing the proven local TUN, smoltcp,
D16, or Endpoint architecture. The design must have a plausible path to the
existing `>170 Mbit/s` local capacity gate and to Knife15's unchanged TCP/UDP
quality envelope before a new long WAN run.

## Failure being solved

Knife15 proved two distinct standard-TUIC limits:

1. an established TCP flow remains bound to one QUIC connection even when
   another connection is healthy;
2. native UDP can lose at the Exit-side socket/QUIC handoff when a single
   path's bounded datagram service stalls.

Future-open load balancing, more pool slots, retries, and local constant tuning
cannot preserve the existing Target TCP socket or give UDP another independent
service path.

## Architecture

```text
TUN / smoltcp / D16 / Endpoint
          |
      OwnedUpstream adapter
          |
     resumable session
       /           \
 ingress A       ingress B       independent provider/ASN paths
       \           /
       session owner             owns Target sockets and replay state
              |
            Target
```

The client maintains at least two end-to-end authenticated transport legs that
terminate at the same logical session owner through distinct L4 ingresses.
Ingresses forward encrypted transport packets; they do not terminate session
authentication, parse application records, or own Target sockets. The owner is
the only authority for a session's Target socket, byte offsets, replay state,
and UDP sequence state.

This is a parallel implementation behind existing `ProxyUpstream` and
`DatagramUpstream` boundaries. `TuicUpstream` remains available unchanged.
`FailoverUpstream` remains future-open failover and is not relabeled as stream
migration.

## Protocol identity and security

- versioned protocol and feature negotiation;
- random 128-bit session id and independent resume secret;
- monotonically increasing leg generation and replay-protected attach nonce;
- TLS 1.3 server authentication plus an exporter-bound per-device credential
  proof; resume authority is minted only inside that authenticated channel;
- per-flow ids are scoped to the authenticated session;
- credentials, resume secrets, and raw authentication records never enter
  logs or evidence bundles.

## TCP ownership

Minimum records:

```text
OPEN(flow_id, target)
C2S_DATA(flow_id, offset, bytes)
S2C_DATA(flow_id, offset, bytes)
ACK(flow_id, direction, next_contiguous_offset)
CLOSE(flow_id, direction, final_offset)
RESET(flow_id, reason)
ATTACH(session_id, leg_generation, proof)
```

The client acknowledges downlink bytes only after the local smoltcp TCP socket
accepts them into its transmit buffer; subsequent packet ownership remains
governed by the existing D16/Endpoint conservation chain. The owner
acknowledges uplink bytes only after the Target socket accepts them. Transport
delivery alone never advances application ownership.

For each direction:

```text
0 <= peer_acked <= next_sent
retained replay bytes == [peer_acked, next_sent)
receiver_accepted <= receiver_contiguous_received <= receiver_highest_received
ACK.next_contiguous_offset == receiver_accepted
```

Sender and receiver observations may lag each other in flight, but an ACK can
never exceed `next_sent`, and an accepted offset never advances across a gap.

Additional invariants:

- exactly one live Target socket is owned by one session owner per TCP flow;
- duplicate or replayed records never duplicate Target or local delivery;
- gaps and conflicting overlaps fail closed;
- a transport-leg failure does not close the Target socket until explicit
  close or the bounded resume grace expires;
- replay-buffer exhaustion backpressures the producer; it never drops TCP;
- only one leg generation owns new transmission after attach commit, while a
  bounded overlap may carry duplicate replay records safely.

Application ACK age, not QUIC ACK alone, is the switch discriminator. An
unnecessary switch is safe because offsets and deduplication make it
non-destructive.

## UDP ownership

Each UDP flow has an owner-assigned packet sequence and deadline. Each leg has
an independent bounded queue and cancellation domain. Receiver feedback
reports contiguous/high-water progress and a bounded loss bitmap.

- one primary leg carries ordinary traffic;
- the secondary stays authenticated and path-probed at low rate;
- feedback stall or excessive loss shifts new packets to the secondary;
- a bounded transition may duplicate packets on both legs;
- the receiver deduplicates before TUN delivery;
- expired packets are counted and dropped explicitly rather than retransmitted
  without bound;
- congestion or queue pressure on one leg cannot block reads or sends on the
  other leg.

Continuous full duplication is not the default. At the Knife15 UDP workload:

```text
23.04 Mbit/s application * 1,207 / 1,160 = 23.98 Mbit/s per active leg
two continuous copies ~= 47.96 Mbit/s
```

The accepted HK reverse direct baseline was `46.080 Mbit/s`, so permanent 2x
duplication would already exceed measured physical capacity. Hot failover and
bounded transition duplication are capacity-plausible; full duplication is
not.

## Capacity and bounds

At the required local `100 Mbit/s` design point:

```text
100 Mbit/s = 12.5 MB/s
500ms retained TCP replay = 6.25 MB per direction
```

Implementation must derive per-flow and global replay bounds from target rate
and accepted resume horizon, then prove them with deterministic tests. The
initial design must support at least a 500ms injected primary blackout at
100 Mbit/s without exceeding a bounded global allocation. It must not freeze
an arbitrary production value before measurement.

Queues are per leg and per traffic class. Control/ACK/attach records cannot be
starved by bulk replay. No queue, dedup window, replay range, pending open, or
resume grace is unbounded.

## Lifecycle and cancellation

- one session supervisor owns both leg tasks and the owner connection;
- leg failure cancels only that leg, not unrelated flows or the owner session;
- flow close is idempotent and carries final offsets;
- explicit owner loss terminates affected flows after bounded grace;
- stale leg generations cannot publish ACK, close, or new-delivery authority;
- task joins, socket ownership, replay bytes, and queue bytes reach zero on
  cleanup.

Process-crash persistence and live Target-socket migration between owner
processes are first-stage non-goals. They require replication below the kernel
socket boundary and are not implied by transport resume.

## Hot-path reachability

Downlink TCP:

```text
Target socket -> owner read -> S2C_DATA -> selected leg -> OwnedUpstream
-> existing relay remote-read buffer -> smoltcp socket send queue
-> iface.poll -> flush_tx -> Endpoint reservation -> TUN
```

Uplink TCP is the reverse path with client retention until owner ACK. UDP uses
the existing TUN flow demux and replaces only the `DatagramUpstream` transport
adapter and server ownership after flow classification.

The change is intended to be sufficient for transport-path continuity, not
merely a necessary instrumentation fix. Real WAN acceptance remains required.

## Observability

Per session, flow, direction, and leg record:

- sent, application-acked, retained, replayed, and deduplicated bytes;
- next offsets, gap/overlap rejection, replay-buffer and queue occupancy;
- leg generation, primary/secondary role, provider/ASN identity, route hash;
- QUIC ACK age separately from application ACK age;
- switch cause, attach latency, blackout duration, and resume outcome;
- UDP ingress/owner/egress sequence counts, feedback gaps, queue drops,
  deadline drops, duplicates, and switch latency;
- Target socket open/close/reset and terminal ownership.

Paired capture remains acceptance evidence. Server socket-overflow and
transport-queue counters become mandatory so a future packet boundary can be
attributed inside the owner rather than only around it.

## Frozen and non-goals

- no D16, Endpoint pacing, MTU/PLPMTUD, pool, QUIC-window, chunk, Cubic, GSO,
  or self-wake tuning;
- no retry of the failed Knife15 resource or relaxation of its SLI;
- no claim that two independent owners can share one kernel Target socket;
- no default all-stream UDP or permanent full-rate duplication;
- no replacement of TUN, fake-IP DNS, or smoltcp;
- no production rollout before deterministic safety and capacity gates pass.

## Failure discriminators

- owner ingress sequence complete but owner egress incomplete: owner queue or
  Target/socket service;
- owner egress complete but client leg ingress incomplete: selected WAN leg;
- client leg ingress complete but TUN delivery incomplete: local relay/D16/
  Endpoint boundary;
- QUIC ACK progresses but application ACK stalls: replay/switch authority;
- replay bytes exceed the derived bound: architecture capacity failure;
- either complete client-to-ingress-to-owner leg shares provider/ASN/route fate
  with the other: resource-admission failure;
- duplicate Target or local bytes: protocol correctness failure.

## Acceptance gates

1. Protocol codec/auth/session tests reject malformed, replayed, stale, gap,
   overlap, and cross-session records.
2. A deterministic two-leg harness injects loss, duplication, reordering, and
   `300..800ms` primary blackout; TCP output is byte-exact, Target opens once,
   no unrelated flow closes, and the maximum complete delivery interruption
   remains below one second.
3. UDP harness proves independent queues, bounded transition duplication,
   exact dedup, deadline drops, and successful secondary-leg delivery when the
   primary is blocked.
4. Loopback real-socket capacity exceeds `170 Mbit/s` with ownership and
   replay bounds intact. Failure is architectural; do not tune unrelated
   constants.
5. A bounded two-ingress VPS qualification proves independent paths, server
   socket/queue counters, paired packet boundaries, cleanup, and unchanged
   safety gates.
6. Only then run the long macOS gate with the existing strict TCP receiver-zero
   and UDP `<=3%` limits.

## Design score

**8/10 before implementation.** The architecture puts socket and byte
ownership at the only layer that can resume established flows, isolates leg
backpressure, has explicit bounds, and preserves the mature local data plane.
It reaches **9/10** after deterministic protocol/capacity proof and **10/10**
only after independent-path WAN acceptance and operational cleanup evidence.
