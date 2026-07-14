# Knife15 Established Relay Lifecycle Architecture Spec

Date: 2026-07-14

Status: **IMPLEMENTED; LOCAL GATES PASS; FRESH MACOS M0 PENDING.** This stage
repairs the macOS M0 failure without changing the accepted Knife14 endpoint
pacing policy or any frozen performance parameter.

## Decision

An open TCP relay is owned by transport and socket lifecycle, not by payload
activity. A relay in the full-duplex `Established` phase therefore has no
payload-idle deadline. The coordinator arms a bounded deadline only when:

1. a local-to-remote write has started and has not completed; or
2. the local write half has completed and the relay is draining the remote
   read half.

The shared policy states are:

```text
EstablishedIdle  -- writer starts work --> WritePending(90s)
WritePending     -- either direction progresses --> WritePending(90s)
WritePending     -- writer completes --> EstablishedIdle(disarmed)
EstablishedIdle/WritePending -- local FIN completes --> HalfClosed(10s)
HalfClosed       -- remote progress --> HalfClosed(10s)
HalfClosed       -- deadline and no owned D16 payload --> close
```

The `90s` value is retained only as the stalled-write no-progress guard. It is
not increased and is no longer a connection-idle lifetime. The existing D16
rule that useful queued or leased downlink payload blocks half-close timeout
remains unchanged.

## Evidence

The first Knife15 M0 run started at 2026-07-14 10:37 local and failed after
about 97 seconds. The mini_vpn log records the TUIC control relay closing at
exactly 90 seconds with:

```text
direction=timer reason=idle_timeout state=Relaying
```

The control stream had only its small target exchange and was legitimately
quiet while the data stream carried the test. The target VPS recorded
`client unexpectedly closed connection` at the matching time. Endpoint pacing
conservation remained bounded at `61,440B`, and TUN/pump error counters did not
select a pacing, capacity, or TUN cause.

This falsifies ADR-0011's assertion that a full-duplex 90-second payload-idle
deadline is safe for transparent long-lived TCP. It does not falsify bounded
cleanup after half-close or while concrete work is stalled.

## Requirements

- Preserve arbitrary transparent TCP sessions, including quiet TUIC control
  streams, long polling, SSE, keepalive-driven protocols, and application
  sessions whose useful silence exceeds 90 seconds.
- Keep a bounded guard for a remote write future that has accepted work but
  stops making progress.
- Keep the existing 10-second half-closed drain deadline and D16 owned-byte
  protection.
- Apply identical semantics to D16, native chunk, native permit, and the
  generic relay engines.
- Keep terminal causes attributable: `stalled_write_timeout` and
  `half_closed_idle_timeout`; the rejected full-open `idle_timeout` cause must
  disappear.
- Preserve channel closure, remote EOF/error, local socket terminal state,
  QUIC connection failure, and explicit shutdown as normal cleanup owners.

## Non-Goals And Frozen Values

- No change to endpoint pacing rates/burst/reservation accounting.
- No change to D16 byte ownership, MTU, pool size, QUIC windows, application
  chunk size, Cubic, GSO default, self-wake, or TUN feedback window.
- No keepalive injection and no arbitrary larger idle constant.
- No attempt to infer application protocol lifetime from payload silence.
- No shortening of the M0 workload to hide the failure.

## Boundary

`RelayCloseTimer` is a small policy object inside the client TCP relay module.
Writer and reader adapters report only lifecycle facts (`write_pending`,
partial `write_progress`, `write_completed`, `read_progress`,
`write_half_closed`). The writer uses an explicit write/flush loop so actual
partial writes reset the no-progress guard; only completed flush disarms it.
The policy owns timer mode, reset rules, and terminal reason. Tokio tasks and
Quinn stream halves remain outer adapters; none of them independently decides
full-open idleness.

This partial boundary is deliberately local: all four current coordinators
consume it, while transport-specific readers and writers stay unchanged apart
from the new pre-write signal.

## Failure Discriminators

| Observation | Meaning |
| --- | --- |
| `stalled_write_timeout` | Concrete upstream write remained pending without bidirectional progress for 90s |
| `half_closed_idle_timeout` | Local FIN completed and remote drain made no progress for 10s, with no D16 owned payload |
| channel/remote/terminal cause | Lifecycle owner closed the relay normally or exceptionally |
| any full-open `idle_timeout` | Regression; rejected architecture is still reachable |

## Acceptance

1. Deterministic generic relay test stays alive beyond the former 90-second
   boundary while both halves are open and quiescent.
2. Exact D16 coordinator test stays alive beyond that boundary and exits when
   its command channel closes.
3. A pending remote write still terminates after 90 seconds without progress,
   and remote progress resets that deadline.
4. Half-close tests retain the 10-second bound and D16 owned-payload rule.
5. All relay engines compile and the root test suite passes.
6. The next macOS M0 must run past 90 seconds without `idle_timeout`; any
   later failure is classified from its own cause rather than treated as proof
   of this fix.

## Architecture Review

System-design and clean-architecture score: **9/10 for this repair scope**.
Requirements, lifecycle ownership, bounds, failure signals, adapters, and
tests are explicit; the policy is shared rather than duplicated across
transport implementations. To reach 10/10, a later lifecycle stage should
give `SocketCtx` an explicit relay-supervisor cancel handle so a local socket
rearm cancels a currently pending remote write immediately. Until then that
rare path remains bounded by the 90-second stalled-write guard rather than by
prompt owner cancellation.
