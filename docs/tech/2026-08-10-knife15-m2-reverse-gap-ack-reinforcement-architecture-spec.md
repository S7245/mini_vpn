# Knife15 M2 Reverse Gap ACK Reinforcement Architecture Spec

Date: 2026-08-10

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

## Decision

Deepen Quinn's existing stream-pressure receive seam and `PendingAcks` module
with one bounded **gap ACK reinforcement**. It is armed only when the peer sends
`STREAM_DATA_BLOCKED` for an open receive stream that currently has both an
ordered assembler gap and buffered tail, the peer's blocked offset covers the
highest received offset, and the Data packet-number history is split. The next
normal ACK reporting that state retains exactly one reinforcement opportunity.
If no later packet causes a newer ACK first, the existing negotiated
`max_ack_delay` timer emits one duplicate ACK and then terminates the
opportunity.

A split packet-number history alone is explicitly ineligible: Quinn's packet
number filter deliberately creates skipped numbers, and a retransmitted STREAM
prefix uses a new packet number while the old number remains absent. Treating
either case as an active STREAM gap would add persistent ACK overhead after
ordinary traffic bursts.

The reinforcement acknowledges only packet numbers already received. It does
not manufacture stream credit, declare peer loss, request path migration,
change congestion state, or retry application payload. A newer normal ACK
replaces the pending opportunity; an ACK generated solely by reinforcement
cannot re-arm itself.

## Paired Failure Evidence

The exact-source `673d13d` Mac artifact
`/tmp/mini_vpn_knife15_macos_20260810_075621.tar.gz` passed baseline, direct,
smoke, all preflights, cycle 1, and cycle 2 forward. Cycle 2 reverse then lost
one complete Target receiver interval while completing the remaining transfer
and cleanup.

The failed conn1 generation 2 stream 2 retained a `Ready` successor service
certificate and did not replace or migrate. Its ordered reader stopped at
`385,286,941B` behind a missing `1,386B` prefix while buffered same-stream tail
grew to `7,552,771B`. Quinn received another `2,982` datagrams and `2,981`
STREAM frames during the pending interval. The prefix arrived after `1,759ms`.

The paired Exit capture identified the exact reverse data socket and zero
kernel drops. Exit-to-client supply stopped for `1,158.373ms` only after the
Exit TCP receive window fell to zero; it reopened immediately before supply
resumed. This selects remote QUIC ordered-loss recovery and the resulting
stream receive-credit exhaustion. It rejects Target, Exit-to-Target TCP,
D16, TUN, Endpoint conservation, certificate admission, replacement,
operator sequencing, and cleanup as owners of the zero interval.

The physical sample also observed transient `.33` probe loss and RTT growth
during the reverse phase. The architecture therefore treats ACK loss as a
bounded, falsifiable contributor, not as already-proven sole cause.

## Goals

1. Prevent loss of the final ACK describing an unresolved receive gap from
   forcing the remote sender to wait for its PTO after tail traffic stops.
2. Keep exact pressure classification in `StreamsState`/the frame handler and
   all bounded scheduling state inside the deep `PendingAcks` module.
3. Add an exact counter so a later qualification can prove whether the new
   branch was reached.
4. Preserve standard immediate/delayed ACK behavior and every existing data
   plane lifecycle.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool, QUIC windows, chunk, Cubic, GSO,
  Endpoint rate/burst, self-wake, workload, rate, or SLIs.
- Do not reactively rebind a path, replace a pool generation, duplicate TCP
  payload, or enlarge receive credit.
- Do not add a user parameter, retry loop, RTT threshold, or application
  ordered-gap policy.
- Do not claim that reinforcement fixes arbitrary WAN loss bursts or is
  sufficient for formal M2.

## Deep Module And Interface

`StreamsState` already owns the exact receive stream and ordered assembler.
It exposes one private predicate for the conjunction of open receive stream,
buffered ordered gap, and credible blocked offset. The existing
`STREAM_DATA_BLOCKED` frame handler is the sole pressure authority.
`PendingAcks` already owns received packet ranges, ACK scheduling, negotiated
frequency, and `MaxAckDelay` timer eligibility; it owns the bounded
reinforcement state. `Connection` continues to ask only whether ACK frames are
sendable and when the next ACK timer expires.

The rejected alternative exposed a public `force_ack` call to the D16
ordered-gap observer. That would leak QUIC packet recovery into the TCP relay,
sample at a slower 250ms cadence, and couple recovery correctness to a
diagnostics switch. Keeping the policy inside `PendingAcks` provides locality
and leaves the interface small.

Clean Architecture score: **10/10**. Refactoring score before the change is
**8/10** because `acks_sent()` currently discards all scheduling state even
when its ACK reports a live gap. Encapsulated arm/fire/disarm state plus tests
brings the module to **10/10** without a new adapter or trait.

## Invariants

1. Reinforcement can be armed only by `STREAM_DATA_BLOCKED` for the exact open
   receive stream while that stream has an ordered gap, buffered tail, and a
   highest received offset no greater than the reported blocked offset.
2. The Data packet ACK history must also have at least two ranges; packet-number
   history by itself is never authority.
3. Reinforcement is scheduled only after a normal ACK covering the armed state
   is sent.
4. At most one reinforcement opportunity exists in the Data packet space.
5. A normal newer ACK replaces the opportunity; it does not accumulate work.
6. A reinforcement ACK consumes the opportunity and cannot re-arm itself.
7. Subtracting confirmed ACK ranges disarms reinforcement when fewer than two
   ranges remain.
8. The timer uses the currently negotiated `max_ack_delay`; no new duration is
   configured.
9. Reinforcement ACKs contain only existing ACK ranges and remain
   non-ack-eliciting.
10. Closing, discarding a packet space, or normal connection teardown drops the
   state with the packet space.
11. The public statistic is monotonic and increments only when the bounded
   reinforcement ACK is actually composed.

## Capacity And Reachability Gate

The failed reverse phase sustained `32.117755 Mbit/s` overall, approximately
`4,014,719B/s`. The frozen stream receive window is about `8MiB`; the observed
`7,552,771B` tail behind one missing prefix was sufficient to stop contiguous
application consumption and exhaust remote stream credit.

One reinforcement is at most one normal QUIC ACK datagram after each terminal
gap-ACK opportunity. It is classified as Endpoint Control and is far below
the frozen `10,240B` control reserve and `30,720,000 wire B/s` endpoint rate.
It introduces no bulk queue and cannot affect the exact 32MiB Endpoint
capacity path.

Affected receive path:

```text
Target TCP -> Exit sing-box/quic-go STREAM -> WAN
  -> Quinn Connection::handle_event / PendingAcks
  -> StreamsState::received / ordered Assembler
  -> D16 ordered reader -> smoltcp -> iface.poll / flush_tx -> macOS TUN
```

The change is intended to be sufficient only for the deterministic branch in
which the last gap ACK is lost after tail traffic stops. It is a necessary,
low-risk discriminator for the paired WAN failure, not proof that every
1-second reverse gap has that cause.

## Old-Path Audit

Quinn packet/time-threshold loss detection, PTO, ACK_FREQUENCY, immediate ACK,
stream flow control, Cubic, MTUD, Endpoint Control/Bulk accounting, successor
service certificate, writer-flight ownership, D16, TUN, and all existing
recovery observers remain active. No old local egress or writer path is
bypassed.

## Failure Discriminators And Stop Rule

- deterministic receive-window exhaustion plus exact stream pressure and
  original-gap-ACK loss recovers through one reinforcement before sender PTO:
  retain the module;
- a historical/skipped packet-number gap without exact stream pressure emits
  no reinforcement: false-positive gate passes;
- no gap ACK is lost: no reinforcement branch is required for correctness;
- a normal newer ACK cancels/replaces the old opportunity and a reinforcement
  never self-rearms: boundedness passes;
- any ACK storm, timer spin, Endpoint/TCP/UDP/TUN regression, or local capacity
  at or below `170 Mbit/s`: reject implementation;
- a paired Mac qualification reaches a nonzero reinforcement count and has no
  complete receiver-zero interval: retain as supporting evidence;
- the same reverse zero interval recurs despite reinforcement: reject this
  mechanism as sufficient and use the new counter plus paired capture to
  select the next architecture; do not tune or repeat unchanged.

Expected RED may enter the minimal implementation. Unexpected regressions
must be repaired at the same invariant or the implementation rejected. After
local gates and review pass, take exactly one paired Mac qualification before
any formal M2.
