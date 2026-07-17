# Knife15 HK macOS M0 Main-Run Results

Date: 2026-07-17

Status: **PASS — two-hour M0 main run accepted; independent fresh rearm later
passed and the full M0 gate is complete**

## Provenance And Prerequisites

- Source: `8bc7b7c6943a83af5fe95297534da89fbf7e0fe7`
- Bundle:
  `/tmp/mini_vpn_knife15_macos_20260717_035908.tar.gz`
- Bundle SHA-256:
  `1ecee823148046fe9f99db639792a1b9c7c2126c98f54d247abababd9c221a88`
- Release binary SHA-256:
  `b895854bb67205c406e7d1e3d1d032dd8beae2f94e2c1d8a652e2d6196d7ae8b`
- Runner SHA-256:
  `9d8c69e213a0513aab1adff7acd38e4f488cb4d588ad543d3b79813f8f975eb6`
- Physical Target and Exit routes: `en1`
- TUN: `utun4`, target-only, MTU `1200`
- DNS evidence target/name: `8.8.8.8` / `example.com`
- Baseline: `/tmp/mini_vpn_knife15_macos_baseline_20260717_014109`
  measured `36.224500 Mbit/s` forward and `9.002664 Mbit/s` reverse.
- The immediately preceding direct discriminator completed at 03:56:01 UTC:
  `679,215,104B`, `18.101918 Mbit/s`, and zero sender/Target-receiver zero
  intervals across 300 seconds. Result SHA-256:
  `d8d36c07507632224b3788dcd33918d8272e2525160612c15428e5b703bf9270`.

The run retained the frozen H10d16, EndpointWindowV1, D16, pool `2`, Cubic,
GSO-enabled, `368,640B` receive-window, `1160B` UDP payload, queue, and
self-wake profile. No constant or acceptance SLI changed.

## Formal M0 Evidence

The controller ran from 04:00:05 through 06:03:38 UTC. It completed eight full
mixed cycles plus the two planned 30-second forward bookends, `6,780s` of
active work, one `300s` idle/resume window, and the `120s` final drain.

- `m0_status=complete`
- phase/health failures: `0/0`
- timeline, result, DNS, and network-control evidence: all `PASS`
- phase result files: `74` total; `66` TCP and `8` UDP
- full-cycle DNS checks: `8/8`
- Target/local direction-aware receiver-zero intervals: `0`
- sender-zero intervals retained for review: `58`
- maximum TCP sender/receiver byte gap: `9,437,184B`
- maximum UDP loss: `1.544587%`

The sender-zero rows do not violate the receiver SLI. Every corresponding
direction-aware receiver interval remained positive and all exact result files
completed. One Exit ICMP control sampled 100% loss while the physical gateway
remained at zero loss; the concurrent receiver intervals remained positive,
so this is retained as transient path evidence rather than a product failure.

## Ownership, Resource, And Cleanup Evidence

- Endpoint conservation passed in every sample and reached the exact maximum
  `61,440B`.
- Final Endpoint state was `61,403/0/0B` available/live/outstanding; maximum
  live/outstanding was only `1,409/1,365B`.
- No endpoint rebind was attempted or needed: `0/0/0` attempts/recoveries/
  failures.
- RSS was `10,240 -> 8,416 KiB`, maximum `39,664 KiB`; file descriptors stayed
  `15 -> 15` with maximum `15`; threads stayed `11 -> 11` with maximum `11`.
- The TUN moved `5,660,666,314B` in and `6,750,602,241B` out with zero sampled
  interface errors. Physical interface errors were also zero.
- Logs stayed inside the envelope with zero compactions. Secret scan passed.
- Stop terminated the process, removed `utun4`, and restored Target, Exit, and
  physical routing without another VPN contaminating the final sample.

## Close-Tail Review

The summary correctly remains `internal_failure_scan: REVIEW`. Nine canonical
command-boundary remote-write failures were Quinn `Stopped(0)`. Every matching
D16 close ended with `queue_queued=0`, `queue_leased=0`,
`queue_reserved=0`, and `queue_closed=true`; no receiver interval failed.
They are the already accepted application/iperf close-tail class, not a data-
plane stall or stranded ownership.

One earlier smoke reverse close released exactly the bounded D16 reservoir
(`permit_terminal_drop_bytes=524288`) plus `27,840B` already undeliverable
after the local application socket became terminal. It occurred before formal
M0 preparation, is byte-for-byte the accepted authoritative rearm-boundary
case, and ended with zero D16 queue ownership. Formal M0 then completed every
phase and final ownership returned to zero. This classified smoke boundary
does not invalidate the M0 main run.

There are no unresolved P0/P1 findings in the reviewed main bundle.

## Decision And Next Gate

The two-hour HK M0 main run and its first stop/cleanup are accepted. This is
the first valid Knife15 M0 workload evidence, but the M0 gate is not fully
closed until a separate fresh `start -> smoke -> stop` bundle proves re-create,
rearm, and cleanup. That second run does not repeat the two-hour workload.

The independent rearm subsequently passed in bundle
`/tmp/mini_vpn_knife15_macos_20260717_063948.tar.gz` (SHA-256 `f4e0f649...`).
The full M0 gate is therefore complete. Use this M0's observed resource/path
envelopes to set the explicit M1 eight-hour SLOs before executing M1. Rearm
result:
`docs/tech/2026-07-17-knife15-hk-m0-rearm-acceptance-results.md`.
