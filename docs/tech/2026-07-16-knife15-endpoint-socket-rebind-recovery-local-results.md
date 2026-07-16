# Knife15 Endpoint Socket-Rebind Recovery Local Results

Date: 2026-07-16

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; SHENZHEN M0 PENDING**

Source baseline: `8e9daf98ac41a6c17dacaeee7255fcaec114d1ac`.

Implementation commit: `0460886`.

Architecture:
`2026-07-16-knife15-endpoint-socket-rebind-recovery-architecture-spec.md`.

## Outcome

The endpoint-owned proactive UDP socket-rebind recovery is implemented. An
active endpoint that continues to transmit but receives no connection traffic
for `clamp(8 * max_rtt, 2s, 7s)` changes only its local UDP socket identity.
It retains the same Quinn Endpoint, both established QUIC connections, active
TUIC streams, congestion/flow state, and EndpointWindowV1 pacing service.

The monitor samples at `250ms`, uses non-blocking pool-slot observation, and
permits at most one rebind in a continuous no-RX episode. Existing connection-
local reconnect remains the later fallback. D16, MTU, pool, QUIC windows,
chunking, Cubic, GSO default, self-wake, endpoint pacing rate/burst, workloads,
and receiver SLI are unchanged.

## TDD Sequence

The first pure fake-time test was RED because the recovery policy did not
exist. The minimum policy then made the one-shot trigger GREEN. A second RED
showed that a replacement connection could inherit the prior connection's
stale deadline; stable-ID replacement now clears that pre-rebind episode.

The two-connection loopback test initially failed because the test server
dropped its Endpoint immediately after `finish`, before the client could
observe the response. Waiting for `SendStream::stopped` fixed the fixture
lifetime. The resulting test proves that both already-established connections
exchange data after the client source port changes and that endpoint pacing
conservation remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

The runner test was RED until summary generation counted rebind attempts,
recoveries, failures, and maximum first-RX latency. A successful recovery log
must include both policy and current-socket generations.

## Review Repair

Code review found one P1 before acceptance. Quinn retains the previous socket
briefly during active migration, so ordinary per-connection RX counters could
increase from an old-socket packet and falsely label the new identity as
recovered.

Vendored Quinn now exposes two endpoint counters:

- cumulative successful socket rebinds;
- the latest rebind generation that received a packet for an existing
  connection on the current socket.

The recovery policy waits for the second counter. Vendored `rebind_recv`
proves old-socket-only traffic cannot advance it, then sends a client path
probe and observes the new-socket response. The mini_vpn policy test separately
proves raw connection RX without the current-socket generation is not a
recovery. No unresolved P0/P1 remains.

## Resource And Capacity Review

The monitor performs at most four samples per second over the frozen two-slot
pool and adds no payload queue, copy, pacing reservation, or relay hot-path
await. A rebind creates one socket at most per episode. The optional bounded
UDP adapter shares its accounting object across socket generations and cannot
reset its service gate.

Endpoint pacing remains exactly:

```text
rate             = 30,720,000 wire B/s
burst            = 61,440B
1ms envelope     = 92,160B
10ms envelope    = 368,640B
application cap  ~= 239.167 Mbit/s
```

This is code-level sufficient proof for changing the socket identity while
preserving live connections. It is necessary-only for real Shenzhen recovery
and does not predict that the WAN will accept the new identity.

## Final Local Gates

- focused recovery policy: `4/4` PASS;
- live two-connection EndpointWindowV1 rebind: PASS;
- bounded send-adapter cross-generation conservation: PASS;
- root library: `646 passed`, `3 ignored`;
- root binary: `2/2` PASS;
- all-target harness check: PASS;
- release build: PASS;
- project Clippy gate: PASS with only established warnings;
- vendored Quinn-proto: `309/309` unit and `3/3` doc PASS;
- vendored Quinn with the explicit local Quinn-proto patch: `29 passed`,
  `3 ignored`, and `1/1` doc PASS;
- Knife15 runner and wrapper self-tests: PASS;
- Knife14 low-RTT, US-client, and sing-box-control self-tests: PASS;
- shell syntax, Rust fmt, and tracked/untracked diff checks: PASS.

No macOS TUN run was performed locally.

## Next Acceptance

Build the pushed commit on the Shenzhen Mac. Because source, runner, and
binary change, take a fresh physical baseline and fresh 300-second direct
continuity discriminator. Only a direct PASS permits start, smoke, and M0.

If no endpoint outage occurs, M0 must retain the existing strict no-receiver-
zero SLI. If rebind occurs, the bundle must show one trigger, no local rebind
failure, and current-socket RX recovery before the 15-second QUIC idle timeout.
Any receiver-zero interval, missing trigger during an evidenced endpoint-wide
outage, missing current-socket recovery, or repeated rebind in one no-progress
episode rejects the architecture. It does not authorize constant tuning.
