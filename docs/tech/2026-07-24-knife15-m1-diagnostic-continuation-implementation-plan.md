# Knife15 M1 Diagnostic Continuation Implementation Plan

Date: 2026-07-24

Status: **Approved for implementation**

## Task 1 — Lock the continuation seam

Add a focused runner self-test whose otherwise-valid receiver evidence
contains one complete zero interval. Under diagnostic policy the phase must
record a typed violation, preserve the JSON, emit phase completion, and
return success. Prove RED before implementation.

## Task 2 — Preserve fail-fast evidence semantics

Implement the default-off continuation predicate and typed TSV writer. Add a
negative fixture proving missing/malformed receiver evidence still returns
failure under diagnostic policy. Re-run the existing M0/M1 fail-fast tests.

## Task 3 — Record UDP SLO observations

Add a valid reverse-UDP fixture above `3.0%`. Record its exact loss and
continue. Prove `3.0%` does not record a violation and invalid UDP evidence
still fails.

## Task 4 — Add the public diagnostic action

Parameterize the existing M1 action without duplicating its preflight,
provenance, schedule, controller, trap, or final-safety paths. Add
`m1-diagnostic` to help, workload identity, status, and CLI dispatch. Keep
formal `m1` defaults and messages unchanged.

## Task 5 — Publish non-acceptance summary semantics

Teach the summary to select the formal or diagnostic event namespace from
`m1-mode`. Publish result-integrity and diagnostic safety evidence, violation
count, terminal diagnostic status, and
`formal_m1_acceptance: NOT_APPLICABLE`. Record an excessive aggregate TCP gap
as a final data-quality violation.

## Task 6 — Complete schedule and safety TDD

Run a compressed diagnostic schedule with receiver-zero and UDP-loss
violations and prove it reaches final drain. Add a command-failure fixture and
prove the same diagnostic schedule stops immediately with `failed`.

## Task 7 — Documentation and operator flow

Update the HITL help and current Knife15 handoff/TODO position. State that the
next user run is `m1-diagnostic`, requires a fresh baseline/direct/start/smoke,
must keep other VPN/TUN software off, and remains non-acceptance regardless of
its data-quality result.

## Task 8 — Local gates and review

Run:

- runner internal and wrapper self-tests;
- shell syntax checks;
- focused repository tests affected by the runner;
- formatting/diff/secret checks;
- staged code review for fail-open behavior, status ambiguity, evidence
  integrity, cleanup, and formal M1 regression.

Repair every P0/P1 before commit. Record learnings/errors, commit one coherent
change, and push the current branch.
