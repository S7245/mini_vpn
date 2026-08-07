# Knife15 M2 Service-Normalized Admission Implementation Plan

Date: 2026-08-07

Status: **LOCAL COMPLETE; ONE MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Source of truth:
`docs/tech/2026-08-07-knife15-m2-service-normalized-admission-architecture-spec.md`.

## Task 1: Preserve And Classify Evidence

- [x] Verify client and paired Exit checksums and exact provenance.
- [x] Verify baseline, direct, smoke, preflight, Endpoint, D16, route, process,
  and cleanup evidence.
- [x] Align qualification control/data opens with pool selection, writer ACK,
  and Exit TCP evidence.
- [x] Reject operator, physical path, Target, Exit kernel, TUN, D16, Endpoint,
  recovery, cleanup, frozen-parameter, and SLI-relaxation branches.

## Task 2: Freeze The Deepened Module

- [x] Define exact service-normalized active ownership without a threshold,
  multiplier, timer, or knob.
- [x] Keep forward qualification outermost and idle/unknown/all-degraded
  fallbacks unchanged.
- [x] Define overflow-safe rational comparison, capacity math, affected hot
  path, logs, discriminators, and one bounded Mac qualification.

## Task 3: Focused RED

- [x] Replace the old unequal-load-first assertion with the exact warm-control/
  cold-data replay and prove current admission selects the cold data path.
- [x] Add exact ratio, max-value, idle, unknown, all-degraded, and preparation
  fallback tests.
- [x] Add diagnostic formatting coverage for normalized selection.

## Task 4: Minimal GREEN

- [x] Add one private overflow-safe rational comparator to path-service policy.
- [x] Apply normalized ordering only to all-busy, all-known, admitted
  candidates after qualification.
- [x] Preserve existing deterministic inner ordering for ties and fallbacks.
- [x] Thread normalized-selection diagnostics through generic and D16 opens.

## Task 5: Gates And Review

- [x] Run focused admission, generation replacement, recovery, and diagnostic
  tests.
- [x] Run exact 32 MiB Endpoint capacity/conservation gate.
- [x] Run root library/main/integration/release/Clippy, Knife15/Knife14 shell,
  vendored Quinn/proto/docs, root docs, fmt/diff/vendor-patch/secret gates.
- [x] Review exact ordering, overflow, unknown/fallback availability, CAS
  races, lock placement, log compatibility, and TCP/UDP/TUN/D16 regressions.
- [x] Repair all P0/P1 findings and rerun affected gates.

## Task 6: Results, Memory, Commit, Push

- [x] Write local results with exact counts and capacity.
- [x] Update `CONTEXT.md`, `TODO.md`, `HANDOFF.md`, `AGENTS.md`, learnings, and
  errors.
- [x] Commit coherent stages and push the current branch.
- [x] Start the paired Exit observer and provide exactly one bounded Mac
  qualification transaction; formal M2 remains blocked.

## Stop Rule

Expected focused RED may enter minimal GREEN. Stop on a frozen-value change,
threshold/weight/timing knob, payload mutation, unbounded state, local 32 MiB
capacity at or below `170 Mbit/s`, or unexpected regression. A failed Mac
qualification must be classified with selected normalized ordering, writer
ACK, and paired Exit evidence before another architecture.
