# Knife14h10d16 Endpoint Pacing Service Local Gate Results

Date: 2026-07-13

Status: **PASS locally; VPS acceptance pending.**

## Outcome

The endpoint-owned pre-accounting architecture is implemented in pinned
`quinn-proto 0.11.16` plus the smallest pinned `quinn 0.11.11` driver adapter.
`EndpointWindowV1` remains default-off; `QuinnDefault` remains the product
default. The accepted fixed candidate and every frozen H10d16 parameter are
unchanged.

The pure service preserves:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

It charges every connection datagram before packet construction, settles the
actual bytes per GSO datagram, retains socket-blocked outstanding bytes,
supports two-connection byte DRR and idle borrowing, reserves bounded control
service, composes path and endpoint deadlines/wakers, and cleans exact state on
cancel, migration, fatal socket outcome, driver drop, and detach. Stateless
endpoint responses require immediate service or are dropped with a counter.

## Fixed Capacity And Deterministic Bounds

- wire rate: `30,720,000B/s`;
- burst: `61,440B`;
- control reserve: `10,240B`;
- bulk quantum: `20,480B`;
- formal connection-datagram bound: `92,160B/1ms`;
- formal connection-datagram bound: `368,640B/10ms`;
- estimated application capacity: about `239.167 Mbit/s`.

Fake-time tests cover exact one- and ten-millisecond windows, arbitrary
datagram sizes, rounding, overflow, time rollback, full-burst socket blocking,
short-build refunds, two-connection fairness, control rotation, deadline/waker
handoff, migration tombstones, detach, and stateless responses.

## Real Local Capacity Gate

The exact GSO-enabled loopback upload used the frozen `32 MiB` payload and
`64 KiB` application chunk with default MTU/PLPMTUD, Cubic, Quinn's unchanged
per-path Pacer, EndpointWindowV1, and no bounded UDP sender.

- application sender: `240.466 Mbit/s` (`>170 Mbit/s`);
- delivered: exactly `33,554,432B`;
- pattern errors: `0`;
- EOF: clean;
- endpoint service: `34,324,276B` / `23,641` grants;
- refunds: `1,867B` / `4` events;
- socket accepted: `34,322,409B` / `23,641` datagrams;
- abandoned: `0B` / `0` datagrams;
- endpoint delay events: `4,965`, maximum `47,253ns`;
- final conservation: `available=61,440B`, `live=0`, `outstanding=0`,
  `records=0`.

The capacity discriminator therefore passes. Constants were not tuned.

## Regression Gates

- vendored quinn-proto: `309/309` unit, `3/3` doc;
- vendored Quinn: `29/29` nonignored unit, `3` expected ignored, `1/1` doc;
- mini_vpn library: `622/622` nonignored, `3` expected ignored;
- all-target harness check: PASS without warnings;
- explicit concurrency: `64/64`, `256/256`, `1024/1024`;
- UDP payload sweep: `500/500` at `1000B`, `1400B`, fragmented `4000B`, and
  fragmented `8000B`, with zero loss or corruption;
- root and focused-vendor format, runner shell syntax/self-test, and diff
  checks: PASS.

No macOS TUN test ran.

## Code Review

The review covered conservation, pre-build reachability, actual GSO segment
settlement, socket `WouldBlock`, control liveness, fairness, waker ownership,
migration, connection drain, default equivalence, D16/TCP/UDP/TUN regression,
observability, and secret exposure.

One defensive P1 was repaired: a path-generation collision previously
detached endpoint pacing and could fail open in release mode. Migration
failure now retains the old attached paced adapter, preserving aggregate
conservation and service even if the internal uniqueness invariant is ever
violated. A focused test proves continued reservation and socket attribution;
all affected suites are green. No unresolved P0/P1 remains.

## Next Gate

Commit the reviewed local implementation, then run one architecture-specific
VPS forward acceptance with `MINI_VPN_TUIC_PACING_POLICY=endpoint-window-v1`,
bounded sender disabled, GSO enabled, and every D16/MTU/pool/window/chunk/
Cubic/self-wake value frozen. Judge the measured window from throughput, TUN
drops, QUIC loss/congestion, endpoint conservation/outstanding counters,
per-connection service, and client/Exit socket evidence. Do not tune constants
if the `>170 Mbit/s` discriminator fails.
