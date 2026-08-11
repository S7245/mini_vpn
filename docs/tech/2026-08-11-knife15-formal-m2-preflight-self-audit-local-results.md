# Knife15 Formal M2 Preflight Self-Audit — Local Results

Date: 2026-08-11

Status: **PASS — formal M2 remains reopened; no qualification repeat or
parameter tuning is authorized**

## Goal

Remove locally detectable causes of another wasted 25-hour Mac transaction
before the already-authorized formal M2. This stage audits the reviewed
successor forward-service inheritance implementation and the exact formal
runner path. It does not change the data plane, workload, SLI, or any frozen
value.

## Findings And Repairs

1. Formal `m2_workload_slo` did not apply the exact recovery-evidence contract
   already used by the two-cycle qualification. A malformed
   `tuic-recovery-evidence` record or a forbidden active
   `tcp_ordered_read_gap` rebind could therefore pass the long-run verdict if
   all aggregate counts remained healthy. Formal M2 and qualification now use
   one `m2_recovery_contract_is_safe` helper.
2. The source floor remained at pre-inheritance revision `5e7a97c`. It now
   requires reviewed inheritance implementation `de4d170` or a descendant.
3. The runner recorded a release binary SHA-256 but did not prove that the
   executable had been rebuilt after tracked Rust/Cargo/vendor inputs, and a
   dirty tracked worktree could invalidate exact-source evidence. Common
   preflight now rejects a stale/symlinked release binary and dirty tracked
   worktree before traffic. Formal M2 repeats the clean-worktree check at its
   action boundary.
4. The Mac HITL runbook still instructed the completed two-cycle
   qualification. It now names the formal `m2` action, the `de4d170` source
   floor, the approximately 25-hour service requirement, and mandatory
   cleanup/finalization.

## TDD Evidence

- RED: the old source floor accepted `5e7a97c`; GREEN rejects it and accepts
  `de4d170`.
- RED: formal M2 had no exact recovery-contract predicate; GREEN accepts empty
  and valid evidence while rejecting malformed evidence and an active ordered
  gap rebind.
- RED: an intentionally old copied release binary and a dirty tracked fixture
  repository were not startup gates; GREEN rejects both and accepts the fresh,
  clean controls.
- The standalone Mac runner self-test and Exit-to-Target observer self-test
  pass. Their timeout/failure fixtures exit successfully only after the
  expected negative branches are observed.

## Local Gates

- Root library: `705 passed; 3 ignored`.
- Main binary: `2 passed`.
- Integration: `10 passed; 4 ignored`.
- Focused successor inheritance: `9 passed`; exact path-reset transaction:
  `1 passed`.
- Release full-TUN 32MiB capacity/EOF invariant: PASS.
- Release forward batch capacity: `1,406.229 Mbit/s`, zero ring drops and
  zero full waits, above the frozen `>170 Mbit/s` stop rule.
- Release build, established Clippy, `cargo fmt --check`, shell syntax, Mac
  runner self-test, observer self-test, and `git diff --check`: PASS. Clippy
  reports only the existing vendored/root warnings.
- No Rust production code, D16, MTU, pool, QUIC windows, chunk, Cubic, GSO,
  Endpoint pacing, self-wake, workload schedule, or SLI changed.

## Review And Release-Readiness Decision

No unresolved P0/P1 remains in the changed runner/runbook scope. The reviewed
successor floor still has exact identity/generation ownership, atomic
path-reset publication, a monotonic dynamic floor, latest-floor install CAS,
one unchanged five-second replacement deadline, and fail-closed loss/path/
close/no-progress handling.

Release-It score for **starting formal M2** is `10/10`. This is not a product
release score: the irreducible remaining gate is the real 24-hour Mac workload
plus cleanup while the Exit and Target remain uninterrupted.

## Exact Next Transaction

On a clean pulled descendant, rebuild release and run exactly once:

`m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2 -> status -> stop`

Do not run `m2-qualification` again. On any failure, preserve
`status/snapshot/stop` evidence and do not retry or tune unchanged. M3 remains
blocked until formal M2 and cleanup pass.
