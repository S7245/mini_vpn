# Knife16 Path-Diverse Resumable Upstream Architecture Specification

Date: 2026-08-18

Status: **TASK 2 LOCAL FIXED CORE COMPLETE; TASK 3 NEXT; M3 BLOCKED**

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

The Task-2 v1 fixed core implements these records:

```text
ATTACH(session_id, nonce, version_range, features, proof)
ATTACH_ACCEPTED(session_id, nonce, selected_version, features)
ATTACH_GENERATION_STATUS(session_id, requested_generation, nonce)
OPEN(flow_id, target)
OPEN_RESULT(flow_id, result)
DATA(flow_id, direction, offset, bytes)
ACK(flow_id, direction, next_contiguous_offset, final_accepted)
CLOSE(flow_id, direction, final_offset)
RESET(flow_id, reason)
```

The fixed frame envelope carries the leg generation. Frame version and
negotiated session version are distinct. `ATTACH_ACCEPTED` echoes the exact
session and request nonce; a proof-valid stale attach can receive an
authenticated, correlated generation status so a lost acceptance response
does not strand the client on an unknowable generation.

`OPEN` domain targets carry the exact non-empty, NUL-free UTF-8 resolver input
in at most 253 bytes. The framing layer neither applies IDNA nor treats the
value as canonical DNS ASCII; trailing dots, Unicode, and other resolver input
remain byte-exact. Resolution policy belongs to the session owner, and an
unresolvable or policy-rejected value produces a stable `OPEN_RESULT` rather
than a protocol parser failure. IP targets and non-zero ports remain encoded
without text conversion.

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
- an in-window gap is buffered within explicit byte/range bounds and grants no
  ACK or delivery authority until filled; out-of-window gaps and conflicting
  overlaps fail closed;
- a transport-leg failure does not close the Target socket until explicit
  close or the bounded resume grace expires;
- replay-buffer exhaustion backpressures the producer; it never drops TCP;
- only one leg generation owns new transmission after attach commit, while a
  bounded overlap may carry duplicate replay records safely.

Application ACK age, not QUIC ACK alone, is the switch discriminator. An
unnecessary switch is safe because offsets and deduplication make it
non-destructive.

## UDP ownership

Each UDP flow has a separate per-direction sequence allocated by that
direction's sender and validated inside owner-authoritative flow state; client
uplink does not require a sequence-allocation round trip. Each packet also has
a sender-relative TTL rather than an unportable absolute monotonic timestamp.
Each leg has an independent bounded queue and cancellation domain. Receiver
feedback reports contiguous/high-water progress and a bounded loss bitmap.

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

Replay capacity uses application bytes and checked integer arithmetic:

```text
bytes/direction = ceil(rate_bps * effective_horizon_ns / 8,000,000,000)
effective_horizon = injected_blackout_budget + max_normal_application_ACK_age

rate       500ms characterization/direction   full duplex
100 Mbit/s                    6,250,000B       12,500,000B
170 Mbit/s                   10,625,000B       21,250,000B
240 Mbit/s                   15,000,000B       30,000,000B
```

Implementation must derive per-flow and global replay bounds from target rate
and accepted resume horizon, then prove them with deterministic tests. A
500ms value with zero ACK allowance describes total retained coverage only;
support for an additional 500ms blackout must add the measured/configured
normal application-ACK age. The initial design must support at least a 500ms
injected primary blackout at 100 Mbit/s without exceeding a bounded global
allocation. Global capacity derives from aggregate directional rate, never
`per_flow * flow_count`. It must not freeze an arbitrary production value
before measurement.

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

## Task-2 local closure

The transport-independent `resumable` fixed core now contains:

- bounded wire codec and literal golden vectors for every v1 record;
- exporter/device/session/generation-bound attach HMAC plus exact-next atomic
  generation authority and response-loss resynchronization;
- directional TCP replay/reorder/dedup/final-offset ownership windows;
- a pure session reducer whose local completion capabilities survive leg
  replacement while peer mutation requires the exact committed leg;
- checked per-flow/global capacity and physical backing/copy characterization
  at 100/170/240 Mbit/s.

Application ACK is emitted only for bytes the local sink actually accepted;
decode, buffering, and abandon do not advance it. FIN acknowledgement is
two-phase and follows successful sink half-close. Live-flow capacity is
reclaimed into count-bounded tombstones without permitting flow-id reuse.
Receive-budget admission and ownership commit share one TCP-owned algorithm;
its reservation is crate-private and cannot become a public cross-window
capability.

Focused `94/94`, root `807 + 3 ignored`, protocol `19/19`, public API `1/1`,
harness `819 + 3 ignored`, concurrency `10 + 4 ignored`, release, Clippy,
rustdoc, vendored Quinn `40 + 3 ignored`, quinn-proto `330`, fmt, and diff
gates pass. Two independent reviews report no unresolved P0/P1. No production
adapter, socket, two-leg harness, WAN run, or throughput claim exists yet.
Detailed result:
`docs/tech/2026-08-18-knife16-resumable-protocol-capacity-local-results.md`.

## Design score

**9/10 after Task-2 deterministic protocol/capacity proof.** The architecture
puts socket and byte ownership at the only layer that can resume established
flows, isolates leg backpressure, has explicit bounds, and preserves the
mature local data plane. It reaches **10/10** only after the adapter, two-leg
harness, local real-socket capacity gate, independent-path WAN acceptance, and
operational cleanup evidence pass.
