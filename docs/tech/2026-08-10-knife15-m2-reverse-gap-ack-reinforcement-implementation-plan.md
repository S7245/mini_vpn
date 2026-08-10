# Knife15 M2 Reverse Gap ACK Reinforcement Implementation Plan

Date: 2026-08-10

Architecture:
`docs/tech/2026-08-10-knife15-m2-reverse-gap-ack-reinforcement-architecture-spec.md`.

## Task 1 — Preserve and classify exact paired evidence

- [x] Verify Mac artifact SHA-256 and exact source.
- [x] Verify baseline, direct, preflights, phase boundary, and cleanup.
- [x] Stop, transfer, hash, and inspect the exact overlapping Exit observer.
- [x] Align the client ordered gap with the Exit TCP zero window.
- [x] Reject certificate, replacement, Target, D16, TUN, Endpoint, and cleanup
  branches; retain WAN ACK/loss as a falsifiable contributor.

## Task 2 — Freeze the deep recovery seam

- [x] Keep exact pressure classification at the Quinn receive-stream/frame
  seam and scheduling in `PendingAcks`; reject an outer D16 `force_ack` seam.
- [x] Reject packet-number range splitting as sufficient authority because
  skipped numbers and retransmission history make it non-exact.
- [x] Define one opportunity, replacement by newer progress, no self-rearm,
  and disarm after range confirmation.
- [x] Reuse negotiated `max_ack_delay`; freeze all configured values.
- [x] Complete capacity math, old-path audit, and stop rules.

## Task 3 — RED one lost terminal gap ACK

- [x] Establish a real deterministic Quinn pair at `82ms` one-way latency.
- [x] Scale receive/stream credit to 64KiB, raise only test congestion/send
  capacity, send beyond the window, and prove the writer is flow-control
  blocked.
- [x] Send a multi-packet server-to-client stream flight and drop its prefix.
- [x] Inject the production receive-side `STREAM_DATA_BLOCKED` semantics for
  the exact gapped stream before transmitting the resulting gap ACK; drop it.
- [x] Require one repeated ACK at negotiated `max_ack_delay`, before sender
  PTO; verify current Quinn emits none.

## Task 4 — GREEN minimal bounded reinforcement

- [x] Add the private exact stream-gap/blocked-offset predicate and use only
  `STREAM_DATA_BLOCKED` to request reinforcement.
- [x] Add private arm/fire/disarm state to `PendingAcks`.
- [x] Route its deadline through the existing `MaxAckDelay` timer.
- [x] Re-arm only after a normal armed multi-range ACK; never after
  reinforcement.
- [x] Disarm when confirmed ranges collapse below two.
- [x] Add an exact transmitted-reinforcement statistic.

## Task 5 — Lock down boundedness and compatibility

- [x] Prove a reinforcement cannot self-rearm.
- [x] Prove later normal ACK progress replaces rather than accumulates work,
  without allowing a newer ordinary timer arm to overwrite an older deadline.
- [x] Prove packet-number history alone and normal delayed ACK behavior remain
  unchanged.
- [x] Prove range confirmation disarms stale work.
- [x] Prove stream delivery recovers through reinforcement before sender PTO.

## Task 6 — Complete gates and review

- [x] Run focused Quinn RED/GREEN slices after each change.
- [x] Run complete vendored Quinn/proto, root, main, integration, release,
  established Clippy, shell, docs, and exact 32MiB Endpoint gates.
- [x] Run fmt, diff, vendor, generated-file, and changed-content secret checks.
- [x] Review correctness, protocol legality, timer boundedness, hot-path cost,
  Endpoint accounting, TCP/UDP/TUN regression, and missing-test risk.
- [x] Resolve every P0/P1 and record local results plus `.learnings`.

## Task 7 — Commit, push, and one paired qualification

- [x] Update `PATCHES.md` with the exact maintained fork contract.
- [x] Update `HANDOFF.md`, `TODO.md`, and `AGENTS.md` with exact accepted
  position.
- [x] Commit the coherent implementation/evidence stage and push as
  `300fb16`.
- [ ] Start one fresh bounded `.33` observer only when the Mac is ready.
- [ ] Take exactly one `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2-qualification -> status -> stop`.
- [ ] Do not run formal M2 or repeat/tune unchanged.
