# Knife15 M2 Successor Service Certificate Implementation Plan

Date: 2026-08-10

Architecture:
`docs/tech/2026-08-10-knife15-m2-successor-service-certificate-architecture-spec.md`.

## Task 1 — Preserve and classify paired evidence

- [x] Verify SHA-256, exact source, binary, runner, and preflight provenance.
- [x] Stop and bundle the exact overlapping Exit observer.
- [x] Identify the failed socket and align client, Exit supply, and Target ACKs.
- [x] Reject operator, baseline, script, D16, TUN, Endpoint, Exit, Target, and
  cleanup branches.
- [x] Apply the prior stop rule and reject flight ownership as sufficient.

## Task 2 — Freeze the service-certificate architecture

- [x] Define identity/generation/path/floor ownership and Unknown/Ready/Stale.
- [x] Reuse fresh auxiliary replacement; reject an in-place current turn.
- [x] Preserve initial-generation availability and anti-churn behavior.
- [x] Complete capacity math, hot-path, old-path, and failure discriminators.
- [x] Record frozen values and one-run qualification stop rule.

## Task 3 — RED exact stale-successor admission

- [x] Extend the scalar test observation with optional successor certificate.
- [x] Replay floor `24,800B`, current `17,360B`, same identity/path, zero black
  holes, and available replacement authority.
- [x] Require `ReplaceAuxiliary`; verify the current implementation instead
  reserves the stale successor.

## Task 4 — GREEN minimal certificate policy

- [x] Add immutable optional certificate ownership to `TcpPoolGeneration`.
- [x] Populate it only from a successful pre-install successor service turn.
- [x] Sample current path generation/cwnd through the existing Quinn adapter.
- [x] Classify Unknown/Ready/Stale inside `TcpPoolAdmission`.
- [x] Route one stale auxiliary through existing replacement and CAS install.
- [x] Extend exact diagnostics without adding a hot-path allocation or scan.

## Task 5 — Lock down availability and lifecycle

- [x] Ready at/above floor remains admitted despite later congestion events.
- [x] Path/identity mismatch is stale; uncertified initial generations remain
  Unknown.
- [x] Primary, draining-predecessor, all-stale, and replacement-unavailable
  cases retain bounded availability.
- [x] Replacement failure keeps attempt-local qualified-only fallback and no
  same-open second maintenance action.
- [x] Stale predecessor streams drain unchanged; successor owns only new opens.

## Task 6 — Deterministic reachability and complete gates

- [x] Add a realistic `82ms` one-way replay with bounded ordinary loss and
  require fresh qualification plus first `128KiB` service within one second.
- [x] Run focused admission/generation/replacement tests after each slice.
- [x] Run root, main, integration, release, established Clippy, shell, and docs.
- [x] Run complete vendored Quinn/proto gates and exact 32MiB Endpoint capacity.
- [x] Run fmt, diff, vendor, generated-file, and changed-content secret checks.

## Task 7 — Review, memory, commit, and qualification

- [x] Review correctness, performance, concurrency, bounded fallback, D16,
  TUN, TCP/UDP, Endpoint, and missing-test risk; resolve every P0/P1.
- [ ] Update `CONTEXT.md`, `.learnings`, `HANDOFF.md`, and `TODO.md`.
- [ ] Commit coherent implementation/evidence changes and push.
- [ ] Start one fresh bounded `.33` observer only when the Mac is ready.
- [ ] Take exactly one `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2-qualification -> status -> stop`.
- [ ] Do not run formal M2 or repeat/tune unchanged.
