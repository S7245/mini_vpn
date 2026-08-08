# Knife15 M2 Endpoint-Blocked App-Limited Ownership Implementation Plan

Date: 2026-08-08

Architecture:
`docs/tech/2026-08-08-knife15-m2-endpoint-blocked-app-limited-architecture-spec.md`.

## Task 1: Lock The Ownership Failure With One Real Quinn Pair

- [x] Configure Endpoint pacing with one datagram of bulk availability and a realistic RTT.
- [x] Keep ordinary STREAM bytes continuously queued.
- [x] Prove the current implementation fails to grow cwnd after the
  Endpoint-bulk-blocked empty poll.

## Task 2: Apply The Minimal Integration-Seam Repair

- [x] Include Endpoint bulk-blocked state in the existing application-limited
  classification while leaving control-only waits unchanged.
- [x] Add no field, timer, retry, payload, target, configuration, or parameter.
- [x] Make the focused tracer GREEN.

## Task 3: Focused Regression

- [x] Run successor service-turn ACK/loss/one-owner tests.
- [x] Run Endpoint default-off, GSO, reservation, socket-outcome, and
  conservation tests.
- [x] Run Quinn congestion tests and the mini_vpn replacement/fallback tests.

## Task 4: Complete Local Gates

- [x] Run root, main, integration, release, established Clippy, shell, vendored
  Quinn/quinn-proto, docs, fmt, diff, vendor, and secret gates.
- [x] Require exact 32MiB Endpoint capacity `>170 Mbit/s`, exact EOF, zero
  socket would-block, and final ownership `<=61,440/0/0B`.

## Task 5: Review, Memory, Commit, And Push

- [x] Review correctness, ordinary idle behavior, congestion/Endpoint
  ownership, lifecycle, boundedness, and TCP/UDP/TUN/D16 regression risk.
- [x] Resolve every P0/P1.
- [ ] Record results in docs and project learning memory.
- [ ] Commit coherent implementation and evidence tasks and push the branch.

## Task 6: One Paired Mac Qualification

- [ ] Start one fresh bounded `.33` Exit observer only when the Mac is ready.
- [ ] Run exactly `m2-ipv6-check -> baseline -> direct-discriminator -> start
  -> smoke -> m2-qualification -> status -> stop`.
- [ ] Stop/bundle the observer, classify both artifacts, and do not run formal
  M2 unless qualification and cleanup pass.
