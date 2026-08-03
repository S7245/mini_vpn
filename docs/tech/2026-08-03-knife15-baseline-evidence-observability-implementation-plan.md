# Knife15 Direct Baseline Evidence Observability Implementation Plan

Date: 2026-08-03

Status: **COMPLETE — local observer gates pass; fresh M2 evidence still required**

Source spec:
`docs/tech/2026-08-03-knife15-baseline-evidence-observability-architecture-spec.md`

## Task 1 — Freeze The Exact Discriminator

- Record the exact source/runner/jq and byte-identical JSON hashes.
- Replay the production predicate and preserve its `ok/ok` result.
- Reject source drift, upload mutation, complete receiver zero, low speed, and
  partial-tail misclassification.

## Task 2 — RED: Structured Validator Classification

- Add focused fixtures requiring `ok`, `invalid_evidence`,
  `validator_error_rc_5`, and `missing_or_symlink`.
- Require pair classification to preserve direction identity.
- Capture the expected failure before adding the interface.

## Task 3 — GREEN: One Predicate, Two Wrappers

- Move jq execution behind one internal adapter without duplicating the
  direction-aware predicate.
- Add structured file/pair reason functions.
- Keep the existing boolean validator wrappers and make them delegate.

## Task 4 — Baseline Manifest

- Add one manifest writer tolerant of command-failure/missing-file state.
- Record status/reason, source/runner, Target/route, duration/parallelism,
  JSON hashes, validation reasons, and receiver summary.
- Write it before every formal baseline terminal outcome.

## Task 5 — Public Read-Only Replay

- Add `baseline-check` to help and action dispatch.
- Select exactly one stage baseline through the existing selector.
- Print structured provenance/classification and pass only for `ok/ok`.
- Run no iperf, route mutation, DNS mutation, TUN, or sudo operation.

## Task 6 — Self-Tests And Review

- Extend internal fixtures and external help/action coverage.
- Run internal/external Knife15 self-tests and Bash syntax.
- Run diff, secret, and worktree checks.
- Review fail-closed status handling, Bash 3.2 portability, symlink/path safety,
  mutation risk, and compatibility with direct/M0/M1/M2 callers.

## Task 7 — Results, Memory, Commit, And Handoff

- Write local results and update project memory/learnings/errors.
- Commit one coherent observer repair and push the current branch.
- Tell the user to pull, run `baseline-check` on the preserved directory, then
  take fresh baseline/direct after the next IPv6-off transition; never reuse
  evidence across the restored/re-disabled physical link.
