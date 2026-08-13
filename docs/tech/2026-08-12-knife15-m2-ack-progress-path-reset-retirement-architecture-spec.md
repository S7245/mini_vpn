# Knife15 M2 ACK-Progress Path Reset Retirement Architecture Spec

Date: 2026-08-12

Status: **LOCAL TDD, CAPACITY, AND REVIEW PASS; PAIRED M2 QUALIFICATION
REQUIRED; FORMAL M2 AND M3 BLOCKED**

Failure evidence:
`docs/tech/2026-08-12-knife15-m2-ack-progress-path-reset-formal-failure-results.md`.

## Decision

Retire the connection-local `TcpPathDegraded -> ResetConnectionPath ->
Connection::path_changed()` policy from the Endpoint recovery monitor.

The monitor retains exact TCP ACK-stall Endpoint socket rebind and UDP no-RX
rebind. Auxiliary generation replacement, current-service handoff, successor
service proof, and predecessor drain also remain. Native Quinn loss recovery,
Cubic, and PLPMTUD continue to own a live path while exact ACK progress exists.

The new ownership rule is:

```text
exact writer ACK progress
    => no client-forced path_changed() on that stable connection/path
```

The old path-reset branch has no distinct safe operating region. If exact ACK
progress has stopped for the bounded interval, the earlier and stronger
`TcpWriteStall` branch already authorizes Endpoint rebind. If ACK progress is
continuing, the formal artifact proves that resetting the current congestion
state can starve an established flow that cannot be migrated.

## Goals

1. Prevent a black-hole counter increment plus ordinary writer backpressure
   from resetting a current ACK-progressing QUIC path.
2. Preserve exact ACK-stall and UDP no-RX Endpoint rebind behavior.
3. Preserve generation replacement and its current-service handoff for future
   streams.
4. Remove dead path-reset policy, action, state, execution, and tests so the
   rejected permission cannot silently return.
5. Keep native QUIC loss, congestion, and PLPMTUD ownership intact.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD configuration, pool size, QUIC windows,
  chunk, Cubic, GSO, Endpoint rate/burst, self-wake, recovery bounds,
  workload, or SLIs.
- Do not add a rate, cwnd, time, loss, retry, lane, or byte threshold.
- Do not migrate or replay established TUIC TCP streams.
- Do not disable generation replacement, successor certification, or the
  reviewed current-service handoff.
- Do not reinterpret a receiver-zero as acceptable or repeat the rejected
  source unchanged.

## Invariants

1. `EndpointRecoveryState::observe()` cannot emit a connection path-reset
   action.
2. A pending writer with continuing ACK progress and any black-hole counter
   movement produces no destructive recovery action.
3. An exact writer whose ACK progress is stalled for the existing bound still
   selects `TcpWriteStall -> Rebind`.
4. UDP demand plus no receive progress retains the existing bounded rebind.
5. Generation replacement remains identity/generation/path CAS owned and
   publishes the exact current-service floor before successor proof.
6. Existing streams stay with their predecessor until normal drain; no byte
   replay or cross-generation migration is introduced.
7. Endpoint conservation and D16 ownership remain unchanged.

## Capacity And Reachability Gate

The failed formal flow requested `4,194,298 bit/s`, or about `524,287
application B/s`. The action being removed reduced current `cwnd` from
`24,285B` to `12,000B` at about `181ms` RTT. Those windows correspond to only
about `1.073 Mbit/s` and `0.530 Mbit/s` of one-window flight capacity before
protocol overhead. Removing the reset preserves transport-earned service; it
does not synthesize or promise a configured rate.

The frozen Endpoint capacity remains approximately `239 Mbit/s`, far above
the formal workload. The end-to-end hot path remains:

```text
TUN -> smoltcp TCP -> TUIC Connect/D16 writer -> Quinn STREAM
-> native Cubic/loss/PLPMTUD -> Endpoint pacing -> Exit -> Target
```

Continuous progress is still supplied by the relay writer, Quinn driver,
Endpoint pacing service, TUN flush, and ordinary runtime wakes. No old local
egress path is bypassed.

This repair is intended to be sufficient for the exact discriminator that
failed: the client cannot collapse an ACK-progressing established flow's
transport state. It does not prove that all external WAN loss patterns meet
formal M2; paired Mac/Exit acceptance remains required.

## Old-Path Audit

The rejected `TcpPathDegraded` trigger, `ResetConnectionPath` action,
black-hole anchors/consumption state, monitor execution branch, and
slot-owned `path_changed()` adapter are removed. The vendored Quinn primitive
may remain internally tested, but mini_vpn no longer invokes it.

Exact ACK-stall/UDP Endpoint rebind, connection qualification, generation
replacement, service inheritance, certificate admission, transport-write
flight ownership, reverse-gap ACK reinforcement, D16, TUN, Endpoint pacing,
native MTUD, Cubic, GSO, and self-wake remain active.

## Failure Discriminators And Stop Rule

- Focused RED must fail only because ACK-progress plus black-hole advance
  currently emits `ResetConnectionPath`.
- GREEN must prove that case is `None` while exact ACK-stall still emits
  `Rebind`.
- Any remaining production call from mini_vpn to `Connection::path_changed()`
  fails the architecture gate.
- Any D16, Endpoint, TUN, UDP, replacement, cleanup, or lifecycle regression
  stops the stage for causal repair.
- Local 32MiB capacity at or below `170 Mbit/s` rejects the implementation;
  constants must not be tuned.
- A paired qualification receiver-zero with all local controls healthy is a
  fresh architecture failure. Do not restore the retired path reset or tune
  parameters.
