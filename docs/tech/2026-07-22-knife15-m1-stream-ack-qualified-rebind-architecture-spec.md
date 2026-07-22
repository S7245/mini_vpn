# Knife15 M1 stream-ACK-qualified Endpoint rebind architecture

Date: 2026-07-22

## Stage goal

Prevent normal QUIC flow-control/congestion pressure from creating a socket-
rebind loop, while retaining the accepted recovery path for a genuinely
black-holed business TCP stream. The recovery decision must be owned by the
exact send stream whose application writer is blocked.

This specification supersedes only the trigger semantics in
`2026-07-22-knife15-m1-tcp-write-stall-rebind-architecture-spec.md`. The
existing Endpoint socket migration, old-socket retention, authenticated set-
wise recovery, and no-RX trigger remain unchanged.

## Grounding evidence

Exact source `e479013597ffe284325c744f3d04a86016588747` produced HK bundle
`/tmp/mini_vpn_knife15_macos_20260722_094608.tar.gz`, SHA-256
`7443769e1ebc61cae241c6cda221dda1a5221909a23133c1d3e1103c6b023313`.
The operation was correct: fresh physical baseline, 300-second direct PASS,
`start`, smoke, M1, then `status/snapshot/stop` after failure.

- baseline receiver rates were `31.257906/26.416944 Mbit/s` forward/reverse;
- direct delivered `15.621349 Mbit/s` with every complete receiver interval
  positive;
- M1 cycle 1 forward offered `15,628,953 bit/s`, delivered the exact phase
  bytes, but had complete receiver-zero seconds 10, 12, and 14;
- the local sender had 24 zero intervals, concentrated in the first 47 seconds;
- the Endpoint performed seven `tcp_write_stall` rebinds and logged seven
  recoveries, with no rebind failure;
- generations 3 through 7 repeatedly changed source port during the affected
  business flow, even though current-socket connection traffic resumed after
  each migration;
- TUN, Endpoint conservation (`<=61,440B`), routes, physical-interface
  counters, resource envelopes, and cleanup remained healthy.

The old signal equated any continuous `poll_write -> Pending` with path
failure. That is false: Pending expresses application demand beyond current
QUIC admission, while stream ACK progress proves that the current path still
delivers that stream. Connection-level RX recovery proved only shared QUIC
activity and repeatedly discharged the wrong ownership boundary.

## Capacity and reachability

The offer is `1,953,619.125 application B/s`. The frozen `32 MiB` stream send
window represents about `17.175524s` at that rate. A two-second Pending edge is
therefore a normal reachable condition under flow control or congestion; it
cannot alone distinguish a black hole.

The production path is:

1. TUIC Connect creates the Quinn `SendStream` and captures a read-only stream
   progress handle.
2. Generic and native/D16 relay writers install that handle in their local
   `TcpWritePressureWriter`.
3. `poll_write(Pending)` starts per-writer ownership; `Ready`, error, drop, or
   cancellation clears it.
4. Quinn-proto derives monotonic `written_bytes` and `acknowledged_bytes` from
   that stream's send buffer, including out-of-order acknowledged ranges.
5. The unchanged 250ms Endpoint monitor samples all pending writers and resets
   only the matching stream's ACK-stall clock when acknowledged bytes advance.
6. The existing Endpoint recovery state may rebind only when both Pending age
   and exact-stream ACK-stall age reach the existing RTT-derived bound.

The sampler performs at most four samples/second per currently pending writer.
The Ready write path uses writer-local atomics; registration/removal and the
monitor use the bounded registry lock. No payload queue, payload copy, retry
task, timer, or capacity constant is added.

## Invariants

For writer `w` on stream `s`, a TCP-write recovery is eligible only if:

```text
pending_age(w) >= clamp(8 * max_rtt, 2s, 7s)
AND
ack_stall_age(s) >= clamp(8 * max_rtt, 2s, 7s)
AND
(connection, writer, episode) is not already covered
```

- ACKs on another stream or connection do not reset `ack_stall_age(s)`.
- ACK progress resets only the ACK-stall clock; it does not release Pending
  ownership.
- one rebind covers every writer episode present in that Endpoint sample;
- a cleared writer or a new episode may arm later recovery;
- sample failure is conservative and cannot manufacture ACK progress;
- `available_tokens + live_reservation_bytes + outstanding_bytes <=
  burst_bytes` remains unchanged.

## Non-goals and frozen values

This stage does not change D16, MTU1200, UDP1160, pool size 2, QUIC windows,
chunk sizes, Cubic, GSO default, self-wake, Endpoint pacing, recovery cadence
or bound, M1 workload, or any SLO. It does not waive the independent reverse-
UDP loss SLO and does not claim an eight-hour M1 PASS.

## Necessary versus sufficient

Per-stream ACK qualification is intended to be sufficient for the false-
positive rebind/churn class selected by this bundle. The pre-existing no-RX
and exact-stream ACK-stall paths remain sufficient candidates for their
respective black-hole classes. Only a fresh real HK M1 can validate the WAN,
TUN, QUIC scheduler, and long-duration behavior together.

## Failure discriminators

- Normal pressure: Pending persists, `write_acknowledged` advances, and no
  `tcp_write_stall` rebind occurs.
- True business-stream path stall: Pending and the logged stream's
  acknowledged byte count both remain unchanged for the bound, then one
  rebind occurs.
- Regression: repeated rebinds occur while that same stream's ACK counter
  advances, or one covered writer episode rebinds more than once.
- Insufficient repair: M1 retains complete receiver-zero intervals without a
  qualifying ACK stall/recovery event.
- Architecture failure: the unchanged 32 MiB local capacity gate is
  `<=170 Mbit/s`, conservation fails, or a TUN/UDP/lifecycle regression appears.

## Acceptance

Focused RED/GREEN tests, Quinn/proto tests and docs, root all-target tests,
release, Clippy, shell gates, formatting, secret checks, code review, and the
exact 32 MiB `>170 Mbit/s` capacity gate must pass before another user-run M1.
The external acceptance remains one fresh eight-hour HK M1 with every other
VPN/TUN disabled through `stop`.
