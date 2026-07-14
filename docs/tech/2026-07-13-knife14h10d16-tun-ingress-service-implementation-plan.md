# Knife14h10d16 TUN Ingress Service Implementation Plan

Date: 2026-07-13
Status: **IMPLEMENTED; VPS ARCHITECTURE FAIL; branch closed**

Source of truth:
`docs/tech/2026-07-13-knife14h10d16-tun-ingress-service-architecture-spec.md`.

## Task 1: Freeze The Failed Batch Evidence

- [x] Archive and secret-scan the `d934f12` VPS bundle.
- [x] Record `194 Mbit/s`, TUN drops `0/38`, QUIC loss `11,717,996B`, exact
  batch counters, endpoint conservation, preflight, and cleanup.
- [x] Close batch-only and constant-tuning branches.

## Task 2: Focused RED For Remaining Amplification

- [x] Extend the existing multi-packet drain tracer to count smoltcp poll and
  TUN flush calls.
- [x] Prove current RED: eight TCP packets, one batch relay pass, eight
  poll/flush calls.
- [x] Keep a default one-packet equivalence assertion.

## Task 3: Packet Ownership Extraction

- [x] Add a bounded-by-batch smoltcp-staged FIFO to real and loopback TUN devices.
- [x] Add a narrow `TunIo` staging method; move TCP raw ownership exactly once.
- [x] Make `Device::receive` drain the staged FIFO in order.
- [x] Test prefetch, FIFO order, mixed TCP/UDP/DNS, and SYN-before-poll.

## Task 4: Batch Poll/Flush Service

- [x] Split per-packet classification/staging from smoltcp poll/flush.
- [x] Poll once and flush once after each nonempty TCP batch.
- [x] Preserve existing backlog transitions and one dirty-relay service.
- [x] Turn Task 2 GREEN and add aggregate counters.

## Task 5: Dedicated Bounded Reader Pump

- [x] Introduce the ingress pump over a generic packet reader.
- [x] Split real `AsyncDevice` only for H10d16; retain the default adapter.
- [x] Size the FIFO from the frozen TUN queue estimate (`500` in acceptance).
- [x] Implement full-wait, error, EOF, cancellation, and shutdown accounting.
- [x] Add deterministic pump capacity/lifecycle tests.

## Task 6: Pumped Full-Path Harness

- [x] Route the forward generator through a frozen 500-packet modeled kernel
  ring and the production pump seam.
- [x] Assert zero drops/overwrite, bounded high water, exact ownership, and
  nonzero pump/batch activity.
- [x] Preserve reverse, control, timer, DNS, UDP, and concurrency scenarios.

## Task 7: Exact Local Capacity Gate

- [x] Run exact `32 MiB`, `64 KiB` forward through real Quinn and the complete
  ingress service.
- [x] Require `>170 Mbit/s`, exact bytes/pattern/EOF, zero modeled drops,
  pump high water `<500`, full waits `0`, batch poll/flush equality, and clean
  endpoint/D16 ownership.
- [x] If any criterion fails, classify architecture failure; do not tune.

## Task 8: Full Local Regression Gates

- [x] Focused ingress/pump/TCP/UDP/DNS/TUN/lifecycle tests.
- [x] Root library and harness suites.
- [x] Explicit concurrency `64/256/1024` and UDP sweep.
- [x] Vendored Quinn-proto/Quinn unit and doc suites.
- [x] All-target check, fmt, bash syntax/self-tests, and diff checks.

## Task 9: Review, Memory, And Commit

- [x] Review ordering, exactly-once ownership, boundedness, task cancellation,
  error propagation, no busy wake, default equivalence, cross-platform split,
  D16/endpoint conservation, and observability.
- [x] Resolve every P0/P1, including terminal TUN error fail-closed behavior.
- [x] Update learning/error memory, HANDOFF/TODO/AGENTS, and result docs.
- [x] Commit one coherent reviewed change after a secret/staging audit:
  `20a0f8cca6ae0661497173671ea4f426333d6114`.

## Task 10: Frozen VPS Acceptance

- [x] Deploy an isolated exact commit and verify source/binary/runner hashes.
- [x] Run profile rehearsal and one target-only forward-only P1.
- [x] Require `>170 Mbit/s`, zero TUN drops, QUIC loss `<=16 MiB`, pump FIFO
  below capacity with zero full waits, exact batch attribution, conservation,
  lifecycle, and cleanup. Throughput, loss, attribution, conservation, and
  cleanup passed; safety failed with `419` TUN TX drops, pump `500/500`, and
  `347` full waits.
- [x] Stop this architecture without parameter tuning.
- [x] No macOS TUN.

Result:
`docs/tech/2026-07-13-knife14h10d16-tun-ingress-service-vps-results.md`.
