# Knife15 M2 Quinn Successor Service Turn Architecture Spec

Date: 2026-08-07

Status: **LOCAL IMPLEMENTATION REVIEWED; MAC QUALIFICATION PENDING; FORMAL M2 AND M3 REMAIN BLOCKED**

Failure evidence:
`docs/tech/2026-08-07-knife15-m2-successor-service-readiness-qualification-failure-results.md`.

## Decision

Deepen auxiliary generation replacement at one seam: after a successor has
completed QUIC/TUIC authentication but before it is atomically installed, run
one exact Quinn **successor service turn**.

The turn owns a byte budget equal to the successor's current congestion
window at start. Quinn emits valid ACK-eliciting `PING + PADDING` carriers,
classified as Endpoint bulk traffic, until the planned congestion-controlled
bytes have been sent. The first tagged packet may also coalesce the already
queued TUIC Authenticate STREAM bytes; those control bytes then receive the
same exact ACK/loss proof rather than bypassing it. The turn succeeds only
when every tagged packet byte in that planned flight is acknowledged on the
same live path generation. Any tagged packet loss, connection close,
replacement deadline expiry, or stale generation fails the turn. There is no
retry.

Only a successful turn permits the existing predecessor identity/activity
CAS to install the successor. Failure leaves the predecessor current, creates
no draining generation, transfers no admission activity, and returns the
existing replacement error to the pending open.

## Why This Mechanism

Authentication is a security/protocol readiness signal, not a service
readiness signal. Current `cwnd/RTT` observations also cannot distinguish an
untested fresh path from a service-proven one. A valid ACK-correlated QUIC
flight is the smallest transport-native proof that the successor can consume
one congestion window through the shared socket, Endpoint pacing, network,
peer receive path, and ACK return path.

Alternatives are rejected:

- re-admitting the degraded predecessor contradicts earlier connection-local
  black-hole evidence;
- a static `cwnd`, rate, byte, or elapsed-time floor creates another tuning
  branch and does not prove delivery;
- Target TCP affinity, duplicate opens, racing connections, or replay changes
  payload semantics;
- keeping an extra warm generation expands the frozen pool and unbounded
  lifecycle surface;
- periodic heartbeats or multiple adaptive probe rounds introduce a new
  timer/retry subsystem.

## Goals

1. Prevent authentication-only auxiliary successors from becoming new-open
   owners before one exact congestion-window service turn is delivered.
2. Preserve the current bounded predecessor/successor lifecycle and atomic
   generation installation.
3. Keep readiness transport-native and independent of Target, workload,
   configured rate, or a new threshold.
4. Charge every service-turn datagram through Endpoint pacing as bulk and
   preserve exact byte conservation.
5. Produce exact planned/sent/acked/lost/path-generation evidence.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool, QUIC windows, chunk, Cubic, GSO,
  Endpoint burst/rate, self-wake, recovery bounds, admission ordering,
  startup priority, workload, or SLIs.
- Do not warm initial startup pool connections or ordinary reconnects in this
  stage; the exact failure is auxiliary replacement installation.
- Do not send business STREAM/DATAGRAM bytes or open a Target connection; the
  already queued TUIC Authenticate control stream may share a tagged packet.
- Do not retry a lost turn, add a new timeout, or calculate a target bandwidth.
- Do not claim formal-M2 or general WAN throughput sufficiency.

## Deep Modules And Dependency Direction

The Quinn protocol implementation owns one private service-turn state machine:
`Idle -> Sending -> AwaitingAcks -> Succeeded|Failed`. Packet tagging, exact
ACK/loss accounting, congestion control, and Endpoint pacing stay behind a
single hidden Quinn adapter. The mini_vpn replacement implementation consumes
only an async success/failure result and never sees packet numbers.

The existing `TcpPoolGenerationSlot` remains the sole lifecycle authority.
It invokes no transport detail itself; `TuicUpstream` sequences
authenticate -> service turn -> generation CAS install. Admission remains a
pure scalar policy and cannot start or complete a probe.

Clean Architecture score is **10/10** for this stage: TUIC replacement policy
depends on one hidden transport capability, Quinn owns all packet/ACK detail,
and the lifecycle CAS remains independent. DDIA score is **10/10**: one owner,
bounded bytes, bounded time, fail-closed loss/close behavior, no duplicated
payload, and unchanged predecessor availability until commit.

## Invariants

1. At most one service turn may be live per Quinn connection.
2. Planned bytes are snapshotted from the positive current congestion window;
   no configured constant determines capacity.
3. Only packets explicitly emitted for the turn carry its tag; a tagged packet
   may also carry already queued Authenticate control bytes.
4. Each tagged packet contributes its congestion-controlled encoded size once
   to sent and then at most once to ACK or loss. Close/path change fails the
   whole turn and preserves the exact partial counters.
5. Success requires `sent == planned_covered` and `acked == sent`, with zero
   tagged loss and the original path generation unchanged.
6. A service-turn packet is Endpoint `Bulk`; reservation settlement and socket
   outstanding accounting are identical to application bulk packets.
7. Turn loss is terminal and is not retransmitted as turn authority.
8. Close, path change, or replacement timeout wakes the waiter and cannot
   later publish success.
9. Success is only a prerequisite; the existing predecessor identity,
   activity pointer, generation increment, and draining-slot CAS still decides
   installation.
10. Failure leaves current/draining/admission ownership byte-for-byte
    unchanged.

## Capacity And Reachability Gate

The failed successor exposed:

```text
cwnd:                 12,000B
RTT:                163.342ms
initial service:      73,465B/s
short-flow offer:  3,533,557B/s
first Target second:       0B
```

One fully acknowledged current-cwnd flight is expected under Cubic slow start
to supply ACK evidence and increase the fresh path window from roughly 12KiB
to roughly 24KiB. At the observed RTT that is about `147kB/s`, enough to place
more than one 128KiB receiver interval across the first second when combined
with the unchanged startup stream service. This is intended to be sufficient
only for the exact cold-successor first-interval discriminator. It is not a
promise to sustain the offered rate or pass formal M2.

The exact 32MiB Endpoint local gate remains the general capacity boundary and
must exceed `170 Mbit/s`, complete exact EOF, report zero socket would-block,
and finish at or below `61,440/0/0B`.

Affected hot path:

```text
TcpPoolAdmission::ReplaceAuxiliary
  -> handshake_aux_with_retries (QUIC + TUIC Authenticate)
  -> Quinn Connection::successor_service_turn (hidden adapter)
     -> quinn-proto bounded PING+PADDING flight
     -> EndpointPacingService Bulk reserve/settle/socket outcome
     -> exact ACK/loss/path-generation state
  -> TcpPoolGenerationSlot::install_successor_with (existing CAS)
  -> existing Connect stream / D16 writer / mature TUIC server / Target
```

All payload steps are unchanged.

## Old-Path Audit

Service-normalized admission, forward qualification, generation replacement,
startup priority, exact writer ACK-stall rebind, connection-local path reset,
UDP-demand recovery, D16, Endpoint pacing, and the old draining lifecycle all
remain active. The change deepens only the successor commit boundary; it is
not a full data-plane replacement.

## Observability And Discriminators

Replacement logs must record successor stable ID/generation, planned/sent/
acked/lost bytes, path generation, exact `handshake_ms`, `service_turn_ms`,
whole `replacement_ms`, and terminal reason. Deadline failure must include the
exact partial turn snapshot available at expiry.
The next bounded Mac qualification is decisive:

- service turn completes, successor installs, and no receiver-zero interval
  occurs: retain the contract and proceed to formal M2 planning;
- service turn completes but the same successor still produces a receiver
  zero with continuous exact writer progress: reject this one-turn
  architecture without increasing turns, bytes, or time;
- turn is lost/closed/timed out: confirm fail-closed predecessor ownership;
  classify path availability rather than tune the turn;
- turn is not reached or a different generation serves the failure: classify
  a coverage mismatch;
- Endpoint debt, CAS, lifecycle, UDP, TUN, D16, or cleanup regression rejects
  the implementation.

## TDD And Stop Rule

Focused RED must prove that an authenticated successor cannot install or
reserve before service-turn success; ACK success permits the existing exact
CAS; loss, close, timeout, and stale identity leave the predecessor current.
Protocol tests must prove one-owner admission, exact bounded flight, ACK-only
success, loss terminality, path-generation safety, bulk pacing class, and
Endpoint conservation.

Expected focused RED may enter minimal GREEN. Stop on any new knob, retry,
Target/application probe, pool expansion, frozen-value change, unbounded
state, local Endpoint capacity at or below `170 Mbit/s`, or unexpected
regression. After all local gates and review pass, run exactly one bounded Mac
qualification with a fresh paired Exit observer; do not run formal M2 first.
