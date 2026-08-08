# Knife15 M2 Successor Service-Turn App-Limited Ownership Implementation Plan

Date: 2026-08-08

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; PUBLICATION IN PROGRESS**

Architecture:
`docs/tech/2026-08-08-knife15-m2-successor-service-turn-app-limited-architecture-spec.md`.

## Task 1 — Freeze paired evidence

- [x] Verify the Mac artifact SHA and exact source.
- [x] Stop, bundle, transfer, and verify the paired Exit observer.
- [x] Prove Target ACK continuity and zero capture/kernel drops.
- [x] Reject operator, environment, Target, Exit-to-Target, TUN, Endpoint,
  route, process, and cleanup branches.

## Task 2 — Complete the reachability gate

- [x] Inventory replacement turn through Quinn ACK, Cubic, generation install,
  business stream, Exit socket, and Target.
- [x] Compare the remaining first-interval time with the broken and corrected
  slow-start capacities.
- [x] Classify the correction as sufficient only for the exact bounded
  cold-successor discriminator.
- [x] Freeze all existing values and old paths.

## Task 3 — Focused RED

- [x] Add one real Quinn protocol-pair test at `82ms` one-way latency.
- [x] Require successful exact ACK accounting.
- [x] Require every tagged ACK byte to retain slow-start authority.
- [x] Observe the pre-fix failure at
  `initial=12,000B acked=13,068B final=23,616B`.

## Task 4 — Minimal GREEN

- [x] Use the existing `SentPacket.successor_service_turn` tag at ACK time.
- [x] Override only the later global app-limited snapshot for that tagged ACK.
- [x] Leave Cubic, service-turn target, rounds, deadline, and ordinary packets
  unchanged.
- [x] Pass the focused realistic-RTT regression and existing successor-turn
  tests.

## Task 5 — Local gates

- [x] Run focused mini_vpn replacement/fallback tests.
- [x] Run root, main, integration, release, and established Clippy gates.
- [x] Run full vendored Quinn and quinn-proto suites and docs.
- [x] Run shell, root docs, fmt, diff, vendor, and secret checks.
- [x] Run the exact 32MiB Endpoint capacity gate and record conservation.

## Task 6 — Review

- [x] Review correctness, congestion semantics, lifecycle, Endpoint
  conservation, TCP/UDP/TUN regressions, and missing tests.
- [x] Resolve every P0/P1 before external qualification.

## Task 7 — Memory and publication

- [ ] Record local results and the paired qualification classification.
- [ ] Update `HANDOFF.md`, `TODO.md`, `AGENTS.md`, and learning/error memory.
- [ ] Commit the coherent stage and push the reviewed descendant.

## Task 8 — One bounded Mac qualification

- [ ] Start a fresh bounded `.33` observer only when the Mac is ready.
- [ ] Pull/rebuild the exact pushed descendant.
- [ ] Run exactly one `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2-qualification -> status -> stop`.
- [ ] Stop/bundle the observer and review paired evidence before any formal M2.
