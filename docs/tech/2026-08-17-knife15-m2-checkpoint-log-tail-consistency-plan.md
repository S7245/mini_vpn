# Knife15 M2 Checkpoint Log-Tail Consistency Plan

Date: 2026-08-17

Status: **IN PROGRESS**

## Task 1: Preserve and replay Formal-1 evidence

- [x] Verify both bundle hashes and Exit internal manifest.
- [x] Prove the run reached `idle-1` with healthy post-idle samples and no
  quality or health failure.
- [x] Classify the attempt as invalid evidence, not a candidate rejection.

## Task 2: Complete-record runner TDD

- [x] RED: an incomplete data-plane tail creates unequal sample counts.
- [x] GREEN: replay counts only the same complete prefix as the envelope.
- [x] RED: an incomplete active-lease tail hides the last numeric value.
- [x] GREEN: ignore only the empty fragment; keep complete malformed values
  fail-closed.

## Task 3: Exact evidence-runner bridge TDD

- [x] RED: candidate contract rejects the repaired runner despite exact binary
  and workload/resource/server/observer identity.
- [x] GREEN: map only the qualified/repaired runner hashes with the exact
  release hash into one compatibility class.
- [x] Retain rejection tests for arbitrary source, runner, and binary drift.

## Task 4: Local gates and review

- [x] Run all script and Python self-tests, shell syntax, diff, provenance,
  release hash, and secret checks.
- [x] Concentrated review of concurrent log parsing, ledger scope, lifecycle,
  and operational rollback.
- [x] Record results and learning/error memory; commit and push one coherent
  evidence-control repair.

## Task 5: Dedicated-host preflight

- [ ] Sync the reviewed source to the HK Mac and rebuild the exact release.
- [ ] Disable bounded-window VPS automatic maintenance agents without changing
  sing-box/networking; prove zero failed units and exact service/config hashes.
- [ ] Run fresh detached-control and resource preflights; verify Exit observer
  ownership is absent before start.

## Task 6: Candidate-2 Formal retry

- [ ] Reuse the immutable accepted qualification baseline.
- [ ] Take fresh direct/resource evidence, then start, smoke, fresh observer,
  and Formal 1 under the bridged evidence runner.
- [ ] Monitor without changing source, workload, network, services, or frozen
  values; finalize and analyze both bundles.
