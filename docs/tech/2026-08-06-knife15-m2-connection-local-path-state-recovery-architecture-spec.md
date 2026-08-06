# Knife15 M2 Connection-Local Path-State Recovery Architecture Spec

Date: 2026-08-06

Status: **SELECTED FOR LOCAL TDD. FORMAL M2 REMAINS NOT RUN.**

Source baseline: `bfaba9e906837c3bd0cc510644589468396d840f`.

Failure artifact:
`/tmp/mini_vpn_knife15_macos_20260806_083422.tar.gz` (SHA-256
`74300b2ea9343bad7551922ad166dfe47478a982295d4bf61a01611e5f560133`).

Paired Exit observer:
`/tmp/mini_vpn_knife15_exit_target_observer_20260806_065748.tar.gz`
(SHA-256
`e95880aed15077d5e5274034e56509bf0aa3892ee54c6b8f7adcfac56580dcc9`).

## Decision

Add one bounded, connection-local recovery action to the existing
`EndpointRecoveryState` policy. When one exact TUIC TCP business writer has
remained `Pending` for the existing RTT-derived stall bound and the owning
QUIC connection has added at least one PLPMTUD black-hole detection during
that same pending ownership, reset only that connection's Quinn path state.

The action calls Quinn's existing `Connection::path_changed(now)` mechanism.
It resets the affected connection's congestion controller, RTT estimator, and
MTU-discovery state from its existing `TransportConfig`; it keeps the same
QUIC connection, streams, TUIC relay, UDP socket, source port, payload bytes,
and Target TCP connection.

One stable QUIC identity may receive this action at most once. The authority
is replenished only by connection-generation replacement, not by writer-ready
oscillation, elapsed time, another black-hole increment, or Endpoint rebind.
This is a recovery circuit breaker, not a control loop or tuning parameter.

## Evidence And Selected Failure Class

The exact Mac run passed baseline at `32.551/58.899 Mbit/s`, the 300-second
direct discriminator at `16.268 Mbit/s` receiver without gaps, start/smoke,
every qualification preflight, exact transfer completion, and cleanup. The
first qualification forward phase nevertheless had three complete Target
receiver-zero intervals around phase seconds 101, 103, and 105. The sender
eventually delivered all `610,402,304B`; Endpoint conservation finished at
`61,407/0/0B` and D16 closed cleanly.

The exact data stream used conn1. During the failure window its Quinn
`black_holes_detected` advanced and ultimately reached `144`; its loss reached
`2,147,556B`, while conn0 remained almost idle with zero black holes and zero
loss. The data writer's maximum pending wait was `7,001,335us`.

The paired Exit observer captured the exact Target socket with zero kernel
drops. Exit-to-Target RTT stayed roughly `1–9ms`, cumulative TCP retransmitted
bytes did not increase, the Target ACKed emitted bytes, and the send queue was
only kilobytes. Exit application supply decayed from hundreds of kilobytes per
second to tens of kilobytes per second in both client sender-stall windows;
the Exit forwarded and received ACKs for the bytes it did receive.

Therefore the selected class is sustained client-to-Exit QUIC service
degradation local to an established connection. This evidence rejects:

- operator command error, low Shenzhen/HK bandwidth, or ambient traffic;
- Target observer mismatch or a persistent Exit-to-Target TCP/kernel limit;
- TUN, smoltcp, D16, Endpoint pacing conservation, routes, DNS, or cleanup;
- a new stream placement, startup priority, or auxiliary-generation issue;
- another MTU, pool, window, chunk, Cubic, GSO, pacing, or timer adjustment.

## Goals

1. Recover an already established TUIC TCP stream from the selected
   connection-local path-state failure without replaying application bytes.
2. Restrict the action to an exact currently pressured writer and exact QUIC
   stable identity.
3. Keep hard ACK-stall Endpoint rebind behavior unchanged and higher priority.
4. Make repeated recovery impossible for one stable identity, including when
   the action cannot be applied because the identity disappeared.
5. Keep all policy in the existing pure recovery module; Quinn exposes only a
   thin mechanism adapter.
6. Preserve Endpoint pacing, TCP/UDP relay ownership, generation draining,
   startup service, admission, fake-IP, TUN, and cleanup behavior.
7. Add enough observation to distinguish action issuance, application,
   identity disappearance, and later qualification success/failure.

## Non-Goals And Frozen Values

- Do not change D16, MTU configuration, PLPMTUD configuration, pool size,
  QUIC stream/connection/send windows, application chunk, Cubic, GSO default,
  Endpoint pacing constants, TUN service, or self-wake behavior.
- Do not broaden the shared UDP-socket Endpoint rebind trigger.
- Do not migrate or replay a TUIC TCP stream across QUIC connections. TUIC v5
  and the mature server expose no end-to-end Target-byte ACK contract that
  could make arbitrary replay safe.
- Do not close the affected QUIC connection or reset its application streams.
- Do not add a configurable threshold, cooldown, target rule, timer, or retry
  loop.
- Do not retry formal M2. The next remote run is the bounded qualification
  cycle only.

## Capacity And Reachability Gate

The failed qualification offered about `16.276 Mbit/s`, or
`2,034,447 application B/s`. The existing `32 MiB` local Endpoint gate has
already proved about `239.167 Mbit/s` application capacity, so this stage does
not add or consume a payload queue and does not alter the accepted capacity
path:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

The monitor samples every `250ms` and the pool has at most three live
generations. The new policy work is one bounded scan of those samples and
writer-pressure records. It allocates no payload memory and performs no I/O
under the recovery state mutation.

Exact hot-path reachability remains:

```text
TUN TCP payload
  -> smoltcp relay / D16
  -> TuicTcpStartupWriter / TcpWritePressureAdapter
  -> Quinn SendStream
  -> affected Connection congestion + loss + MTUD state
  -> EndpointPacingService reservation
  -> shared AsyncUdpSocket
  -> mature TUIC Exit
  -> Target TCP
```

`TcpWritePressureAdapter` provides the exact current business-writer
`Pending` evidence. `endpoint_recovery_input` supplies the same connection's
stable identity, RTT, and `black_holes_detected`. `EndpointRecoveryState`
selects the action. A thin Quinn adapter invokes the already implemented
proto `path_changed` reset and wakes the driver.

The stage is intended to be sufficient for the selected invalid/stale Quinn
path-state class, but only the fresh real-WAN qualification can prove that
the observed black-hole process is recoverable rather than continuing
physical UDP loss. It is not sufficient for full 24-hour M2 acceptance.

## Pure Policy Contract

Each live stable identity owns a black-hole anchor and optional consumed
path-reset authority.

- With no currently pending business writer, refresh the anchor to the current
  black-hole count. Historical idle or ambient detections cannot authorize a
  later reset.
- While at least one writer is pending, preserve the anchor. An increase is
  attributable to that ownership window.
- Counter regression is unknown evidence: refresh the anchor and issue no
  action.
- A connection-local reset is eligible only when:

```text
pending_for >= clamp(8 * max_connection_rtt, 2s, 7s)
black_holes_detected > pending_window_anchor
stable_identity has not consumed path-reset authority
```

- If the existing exact writer ACK-stall predicate is also eligible, its
  established Endpoint rebind action wins. Connection-local reset addresses
  the trickle-ACK gap only; it does not weaken the complete-stall remedy.
- Issuing the action consumes authority before the adapter call. Missing or
  closed connection state cannot create a retry loop.
- Remove anchors and consumed authority only after the stable identity leaves
  the sampled live set. A replacement generation receives a new stable id and
  new authority.
- Among multiple eligible connections, choose the longest current pending
  duration, then largest black-hole advance, then stable identity for a
  deterministic bounded decision.

## Quinn Adapter

Pinned `quinn 0.11.11` exposes one hidden `Connection` operation that locks
only that connection, calls proto `path_changed(runtime.now())`, and wakes the
existing driver. It contains no detection policy or constants.

`TuicUpstream` captures the exact `Connection` handle, pool index, and pool
generation in the same nonblocking snapshot that supplies the pure-policy
sample. It invokes only the handle whose stable id the policy selected, with
no second pool lookup and without holding pure-policy state across the
connection lock. The result is one of:

- `applied`: exact live identity reset and driver woken;
- `closed`: the exact captured connection closed before application;
- `not_found`: the selected identity was absent from the captured handle set.

All results keep the one-shot authority consumed and are logged without
secrets.

## Existing Paths That Remain Active

- Exact writer ACK-stall plus existing RTT bound may rebind the shared
  Endpoint socket; it retains precedence.
- Demand-qualified generic UDP no-RX recovery remains unchanged.
- Busy-epoch forward qualification and auxiliary generation replacement
  continue to isolate only new opens.
- New-stream startup service remains exactly one first-payload scheduling
  turn.
- Endpoint byte-owned pacing, D16, smoltcp/TUN service, QUIC loss recovery,
  Cubic, PLPMTUD, and mature-server copy service remain active.

The old paths are complementary. None is described as a full replacement for
the new established-stream recovery seam.

## Deterministic TDD And Failure Discriminators

Required RED/GREEN tests:

1. ACK progress plus bounded Pending plus same-window black-hole advance emits
   exactly one `ResetConnectionPath`, not Endpoint rebind.
2. Pending without black-hole advance emits no action.
3. Black-hole advance without current Pending refreshes/clears ownership and
   emits no action.
4. Counter regression emits no action.
5. One stable identity cannot reset twice across more black holes or writer
   episodes; an identity replacement can reset once.
6. A different healthy connection is never selected or modified.
7. Exact ACK stall retains Endpoint-rebind precedence.
8. Vendored Quinn reset restores configured path state and permits the same
   connection/stream to continue delivering bytes.

The next real qualification must correlate:

- `tuic-connection-path-reset` stable id, writer, stream, episode, pending
  bound, black-hole anchor/current, and apply result;
- per-connection RTT/cwnd/loss/black-hole/MTU counters before and after;
- exact writer acknowledged bytes and maximum wait;
- Endpoint conservation and D16 lifecycle;
- Exit TCP_INFO/pcap or an equivalent exact observer when available;
- Target receiver-zero intervals.

If the exact action applies but the same healthy-control Target receiver-zero
failure recurs, reject this architecture as insufficient. Do not add retries
or tune constants; next investigate sustained physical UDP loss versus
mature-server/Quinn delivery recovery. If the action never becomes eligible,
the discriminator is an observer/policy mismatch, not permission to broaden
the trigger.

## Architecture Ratings

- Clean Architecture: **8/10 before implementation; target 10/10**. Detection
  and invariants remain in a pure module and the framework adapter is thin;
  the missing tested Quinn adapter and connection lookup are the remaining
  boundary work.
- DDIA fault tolerance: **8/10 before implementation; target 10/10**. The
  failure is isolated to one identity and the recovery is idempotent and
  bounded; real-WAN qualification is still required to validate liveness.
- Deep-module opportunity: **9/10 before implementation; target 10/10**. The
  existing Endpoint recovery seam can own the policy without leaking
  black-hole logic into D16, TUN, relay writers, or runner scripts.
