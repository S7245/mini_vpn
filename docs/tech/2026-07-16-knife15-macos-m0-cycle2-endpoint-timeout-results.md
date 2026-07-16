# Knife15 macOS M0 Cycle-2 Shared Endpoint Timeout Results

Date: 2026-07-16

Status: **M0 FAILED; OPERATION CORRECT; ENDPOINT-WIDE UDP RECEIVE OUTAGE
SELECTED; PREVIOUS LOCAL CLOSE REPAIR ACCEPTED**

## Evidence And Provenance

The synchronized Shenzhen bundle is:

- `/tmp/mini_vpn_knife15_macos_20260716_110535.tar.gz`;
- SHA-256:
  `f979971173638f41798d9d260e94ca084412b6279866b3ae7826866a01349ccb`;
- source: `8e9daf98ac41a6c17dacaeee7255fcaec114d1ac`;
- release binary SHA-256:
  `9368a10a...`;
- runner SHA-256:
  `c51d2d5...`.

The archive checksum is exact. Its manifest, logs, iperf results, network
observer, and final snapshots are structurally complete. The run used the
fresh physical baseline
`/tmp/mini_vpn_knife15_macos_baseline_20260716_105307` and the passing
300-second direct discriminator
`/tmp/mini_vpn_knife15_macos_direct_20260716_105537`.

The direct Target receiver delivered `589,692,928B` at `15.716047 Mbit/s`
with no sender or receiver zero interval. Slow Shenzhen capacity remains an
environment measurement, not a product failure.

## Operation And Timeline

The user operation was correct and did not stop M0 early:

- start became ready on `utun5` at `11:05:36Z`;
- smoke ran from `11:05:38Z` to `11:06:23Z` and passed both directions;
- M0 began at `11:08:03Z`;
- cycle 1 completed at `11:22:32Z`, including 300-second forward and reverse
  TCP, 180-second reverse UDP, six alternating short flows, and DNS;
- cycle 2 forward began at `11:22:32Z` and the workload failed by itself at
  `11:23:37Z`, about 65 seconds later;
- the later snapshot at `11:29:55Z` and stop at `11:30:02Z` preserved evidence
  and cleaned the TUN.

The run therefore ended in roughly 15 minutes because the second cycle
failed. It was not a 30-minute workload interrupted by the user, a late sudo
password, missing client process, stale direct result, route error, or early
cleanup.

## Exact Failure

Cycle 2 forward's client JSON reports `control socket has closed
unexpectedly`. It contains 64 intervals, 26 of them zero. The first sender
zero begins at about 38 seconds and the client had sent `73,007,104B` before
the control connection was lost.

The Target service independently reports that the client unexpectedly closed
the connection. Its record contains 48 receiver intervals, 14 of them zero,
and `63,379,498B` received. The Target iperf service remained active with zero
restarts.

The TUIC topology makes the failure endpoint-wide:

- conn0/stream11 carried the quiet iperf control flow;
- conn1/stream10 carried the forward data flow;
- both connections shared one Quinn Endpoint, UDP socket, and Shenzhen source
  port `64195`;
- both connections ended `TimedOut` together;
- connection-local reconnects reused that Endpoint/socket identity and hit
  the five-second handshake timeout repeatedly.

The Exit sing-box service remained active with zero restarts. Its `+0800`
logs record both cycle-2 Target opens from source port `64195` at
`19:22:32/33`, after the same socket had already served smoke and all of cycle
1. This rejects a permanent startup blackhole or authentication failure.

## Negative Controls

The failure is not selected by local pacing, TUN, resource, or general network
signals:

- endpoint pacing conservation remained at or below `61,440B` and ended
  `61,440/0/0B` available/live/outstanding;
- endpoint `would_block=0`, maximum pacing delay `47us`, and no invariant
  breach occurred;
- TUN pump high-water was `102/500`, with zero full waits, pump errors,
  interface errors, and flush failures;
- file descriptors and threads remained stable;
- Exit and gateway ICMP stayed available through the failure window, with
  physical `en0` errors at zero;
- physical and TUN throughput dropped only when the shared QUIC endpoint
  stopped carrying traffic;
- Exit sing-box and Target iperf remained active with zero restarts.

ICMP cannot prove UDP `8443`, but the combined controls reject a general Mac
network outage, service crash, local event-loop pressure, pacing starvation,
or TUN backpressure as the immediate cause.

The summary's `m0_receiver_zero_intervals=0` must not be used: the invalid
client JSON was deliberately excluded by the aggregate validator. The Target
journal supplies the valid receiver evidence of 14 zero intervals.

## Accepted Classification

The old source port was healthy for roughly 18 minutes, then the whole shared
UDP endpoint lost useful receive/service progress. Both established QUIC
connections timed out, and reconnect on the same socket identity could not
recover. A fresh process in the preceding rearm evidence used a new source
port and recovered, so socket identity is the smallest remaining recovery
boundary.

The exact WAN component that discarded late UDP is not observable without a
packet capture covering this failure instant. That distinction is no longer
required for the client architecture decision: whether the loss is a NAT/
classifier mapping or a local socket receive failure, retrying new QUIC
connections on the unchanged Endpoint/socket cannot restore service.

Commit `9f68435` is independently accepted by this incident. When transport
failed, the same-epoch close coalescing delivered a local failure and iperf
exited at about 65 seconds instead of hanging indefinitely.

## Decision And Stop Rule

Do not rerun unchanged M0 and do not tune pacing, D16, MTU, pool, QUIC
windows, chunking, Cubic, GSO, self-wake, workload rates, or receiver SLI.

Proceed with the endpoint-owned socket-rebind recovery architecture in
`2026-07-16-knife15-endpoint-socket-rebind-recovery-architecture-spec.md`.
Local deterministic tests must prove one-shot detection, multi-connection
continuity, preserved endpoint pacing conservation, and bounded cleanup.

After local gates and review pass, the next Shenzhen M0 is the real-path
acceptance. If it still produces a receiver-zero interval without a rebind, if
rebind occurs but no receive progress resumes, or if repeated rebinds occur in
one no-progress episode, classify the architecture as failed. Do not adjust
the new liveness constants from one WAN result.
