# Knife15 M2 Ordered-Gap Path Migration Implementation Plan

Date: 2026-08-06

Status: **LOCAL IMPLEMENTATION COMPLETE; FORMAL M2 REMAINS BLOCKED**

Source of truth:
`docs/tech/2026-08-06-knife15-m2-ordered-gap-path-migration-architecture-spec.md`.

Goal: add exact ordered-gap observation and one bounded active-migration
action without changing frozen values or replaying application payload.

## Task 1: Preserve And Classify Evidence

- [x] Verify artifact SHA, source, binary, runner, baseline/direct, and cleanup.
- [x] Locate formal M2 failure at cycle 2 reverse TCP, about 25 minutes into
  the schedule, and distinguish the later fourteen-hour evidence hold.
- [x] Prove eleven complete receiver-zero intervals and the exact 12,630ms
  stream read gap.
- [x] Reject TUN, D16, Endpoint pacing, routes, process, cleanup, operator,
  and frozen-parameter branches.
- [x] Identify buffered/fresh connection STREAM evidence ahead of the ordered
  prefix and the shared-path congestion collapse.

## Task 2: Freeze Architecture And Vocabulary

- [x] Write goals, non-goals, capacity, hot path, old-path audit, pure policy,
  TDD, qualification, and stop rules.
- [x] Select exact ordered-gap Endpoint migration.
- [x] Reject generic TCP silence, client-only path reset, parameter tuning,
  and cross-transport stream replay.
- [x] Add the accepted term to `CONTEXT.md` after GREEN.

## Task 3: Quinn Receive-Progress RED/GREEN

- [x] RED ordered assembler gap -> exact progress snapshot.
- [x] RED contiguous, empty, consumed, and closed stream cases.
- [x] GREEN a thin proto snapshot plus cloneable high-level
  `RecvStreamProgress` handle.
- [x] Verify the handle is read-only and contains no recovery policy.
- [x] Update vendored patch manifest/checksum evidence.

## Task 4: TUIC Read-Pressure Registry RED/GREEN

- [x] Add one per-generation bounded registry for live D16 ordered readers.
- [x] Register the progress handle at relay construction and remove it at
  reader drop without adding a read-hot-path lock.
- [x] Sample raw ordered-gap progress with current/draining generations.
- [x] Keep unordered/generic modes unchanged and ineligible.

## Task 5: Pure Recovery Policy RED/GREEN

- [x] RED first observation -> none; same gap next sampler turn -> one rebind.
- [x] RED progress, changed gap, close, replacement, quiet stream, and covered
  episode cases.
- [x] RED later greater-offset gap -> one new episode.
- [x] RED writer ACK-stall precedence and prior path-reset behavior.
- [x] GREEN exact anchors, deterministic selection, and covered authority.

## Task 6: Action And Observation

- [x] Apply only the existing Endpoint rebind mechanism.
- [x] Log exact stable id, reader, stream, episode, read/next/highest offsets,
  buffered/gap bytes, observation count, socket generation, and result.
- [x] Preserve old-socket authentication/drain and recovery accounting.
- [x] Add a real Quinn rebind-and-continue stream test or extend the existing
  one without adding policy to Quinn.

## Task 7: Bounded Qualification Runner

- [x] Add a public exact two-cycle ordered-gap recovery qualification using
  the formal workload phases and all existing fail-closed validators.
- [x] Mark success `PASS_NON_ACCEPTANCE`; never promote it to formal M2.
- [x] Preserve status/snapshot/stop evidence after failure and full cleanup.
- [x] Add shell self-tests for schedule, source binding, and verdict.

## Task 8: Focused, Capacity, And Full Gates

- [x] Run focused proto/Quinn/TUIC recovery and regression tests.
- [x] Run exact 32 MiB Endpoint gate; require `>170 Mbit/s`, exact bytes/EOF,
  zero would-block, and final `61,440/0/0B` conservation.
- [x] Run root library/main/integration/release/Clippy gates.
- [x] Run Knife15/Knife14 shell syntax/self-tests, vendored Quinn/proto tests
  and docs, root docs, fmt/diff/patch/secret checks.
- [x] Stop and diagnose any unexpected regression; do not tune constants.

## Task 9: Review, Results, Memory, Commit, Push

- [x] Review correctness, lock ordering, boundedness, stale offsets, stream
  close/reuse, action precedence, migration lifecycle, false positives,
  D16/TUN/UDP/TCP/Endpoint regressions, and missing tests.
- [x] Repair all P0/P1 findings and rerun affected gates.
- [x] Write failure/local results and exact counts/rates.
- [x] Update `TODO.md`, `HANDOFF.md`, `AGENTS.md`, `CONTEXT.md`, learnings,
  and errors.
- [x] Commit coherent stages and push the current branch.
- [x] Provide exactly one bounded two-cycle Mac qualification transaction.

## Stop Rule

Expected focused RED may enter its minimal GREEN. A local capacity result at
or below `170 Mbit/s`, payload replay, ordinary TCP-silence trigger, frozen
value change, unbounded migration, or unexpected regression rejects the
implementation path. After local gates, the next Mac run is the bounded
two-cycle qualification only; formal M2 stays blocked until that result and
cleanup pass.
