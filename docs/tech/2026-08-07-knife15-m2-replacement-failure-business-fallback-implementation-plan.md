# Knife15 M2 Replacement-Failure Business Fallback Implementation Plan

Date: 2026-08-07

Status: **LOCAL DELIVERY COMPLETE; MAC QUALIFICATION PENDING; FORMAL M2 AND M3 BLOCKED**

Architecture:
`docs/tech/2026-08-07-knife15-m2-replacement-failure-business-fallback-architecture-spec.md`.

## Task 1: Preserve And Classify Evidence

- [x] Verify Mac and Exit artifact checksums and exact source.
- [x] Validate baseline, direct, smoke, forward completion, reverse no-connect,
  service-turn loss, later independent replacement, Endpoint, TUN, routes,
  process, and cleanup.
- [x] Reject operator, sustained WAN/VPS outage, Exit-to-Target, D16, Endpoint,
  selector, frozen parameter, and service-turn-tuning branches.

## Task 2: Freeze The Contract

- [x] Keep service-turn failure terminal and successor installation forbidden.
- [x] Define attempt-local failed-slot exclusion and qualified-only business
  fallback.
- [x] Forbid same-open replacement retry, Target/payload replay, new timers,
  pool growth, and frozen-value changes.
- [x] Preserve later independent replacement authority.

## Task 3: Focused RED

- [x] Add a replay where a degraded auxiliary requests replacement and the
  simulated failure must fall back to the qualified current generation.
- [x] Extend the replay to three slots so another degraded auxiliary cannot
  receive a second replacement attempt from the same business open.
- [x] Lock preparation `Busy`, exact ownership increment/drop, and fresh
  authority for a later open.

## Task 4: Minimal GREEN

- [x] Add bounded per-open failed-slot state to reservation acquisition.
- [x] Extend admission with slot exclusion and post-failure qualified-only
  selection.
- [x] Continue admission after replacement failure instead of returning the
  maintenance error directly.
- [x] Preserve exact combined error when no safe fallback exists.
- [x] Add bounded failure/success diagnostics with
  `same_open_retry=false`.

## Task 5: Gates And Review

- [x] Run focused fallback, pool, recovery, and service-turn suites.
- [x] Run root library/main/integration, release, and established Clippy.
- [x] Run exact 32 MiB Endpoint capacity/conservation gate.
- [x] Run Knife15/Knife14/Exit shell and vendored Quinn/proto/doc gates.
- [x] Run fmt/diff/vendor/secret checks.
- [x] Review concurrency, wake ordering, multi-slot behavior, lease accounting,
  fallback safety, no-retry authority, diagnostics, and TCP/UDP/TUN/D16 risk.
- [x] Repair every P0/P1 finding and rerun affected full gates.

## Task 6: Results And Qualification

- [x] Record failure, architecture, local results, project learnings, and
  command errors.
- [x] Commit the coherent Rust/TDD change.
- [x] Commit/push project documents and memory.
- [ ] Start a fresh bounded `.33` observer when the Mac operator is ready.
- [ ] Take exactly one fresh Mac qualification and preserve cleanup evidence.

## Stop Rule

Expected RED may enter minimal GREEN. Stop on a new knob, timer, retry, pool
expansion, Target probe, payload replay, unbounded state, degraded/unknown
same-open fallback, local 32 MiB capacity `<=170 Mbit/s`, unexpected
regression, or a repeated Mac no-connect discriminator after an observed
qualified fallback. Do not run formal M2 or M3 before qualification and
cleanup pass.
