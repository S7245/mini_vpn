# Knife15 M2 Recovery Evidence Observer Implementation Plan

Date: 2026-08-07

Status: **LOCAL IMPLEMENTATION COMPLETE; MAC QUALIFICATION REQUIRED; FORMAL M2
REMAINS BLOCKED**

Source of truth:
`docs/tech/2026-08-07-knife15-m2-recovery-evidence-observer-architecture-spec.md`.

## Task 1: Preserve And Classify Evidence

- [x] Verify bundle checksum, exact source/binary/runner provenance, baseline,
  direct discriminator, phase results, and cleanup.
- [x] Prove six ordered-gap rebinds occurred on a passing long stream.
- [x] Prove the failed short flow was uplink and ordered-gap-ineligible.
- [x] Reject operator, physical continuity, TUN, D16, Endpoint capacity,
  cleanup, and frozen-parameter branches.

## Task 2: Freeze Repair Architecture

- [x] Reject timeout tuning, generic TCP silence, stream replay, and another
  active ordered-gap predicate.
- [x] Define a pure evidence observer separate from recovery authority.
- [x] Define bounded writer/ACK and ordered-gap episode contracts.
- [x] Define one short differential Mac qualification with paired Exit
  evidence; formal M2 stays blocked.

## Task 3: Focused RED

- [x] Change the persistent ordered-gap test to require evidence plus no
  Endpoint action.
- [x] Add writer episode start/ACK aggregation/end tests.
- [x] Add identity/episode replacement and stale-anchor cleanup tests.
- [x] Keep existing hard writer rebind and connection-local reset tests green.

## Task 4: Minimal GREEN

- [x] Remove ordered-gap fields and actions from `EndpointRecoveryState` and
  `EndpointRecoveryTrigger`.
- [x] Add the bounded pure `RecoveryEvidenceObserver` module.
- [x] Format observer events in the existing recovery sampler adapter.
- [x] Preserve Quinn receive progress, D16 registry lifecycle, writer
  progress handles, and all frozen values.

## Task 5: Gates And Review

- [x] Run focused TUIC/Quinn/proto tests.
- [x] Run exact 32 MiB Endpoint capacity and conservation gate.
- [x] Run root library/main/integration/release/Clippy, Knife15/Knife14 shell,
  vendored Quinn/proto, docs, fmt/diff/patch/secret gates.
- [x] Review false positives, boundedness, identity reuse, lock ordering,
  logging volume, action precedence, and TCP/UDP/TUN/D16 regressions.
- [x] Repair all P0/P1 findings and rerun affected gates.

## Task 6: Results, Memory, Commit, Push

- [x] Write exact local results and gate counts.
- [x] Update `CONTEXT.md`, `TODO.md`, `HANDOFF.md`, `AGENTS.md`, learnings,
  and errors.
- [ ] Commit coherent stages and push the current branch.
- [ ] Provide exactly one bounded Mac qualification transaction with the Exit
  observer; do not run formal M2.

## Stop Rule

Expected focused RED may enter the minimal GREEN. Any active action derived
from ordered-gap evidence, frozen-value change, payload replay, unbounded log
growth, local capacity at or below `170 Mbit/s`, or unexpected regression
rejects the implementation path. A failed short qualification must be
classified with exact writer/ACK plus Exit evidence before another recovery
mechanism is selected.
