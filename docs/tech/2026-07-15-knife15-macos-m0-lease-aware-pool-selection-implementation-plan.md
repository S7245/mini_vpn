# Knife15 macOS M0 Lease-Aware TCP Pool Selection Implementation Plan

Date: 2026-07-15

Status: **COMPLETE; COMMIT `c945a41`; FRESH USER-RUN M0 PENDING**

Source spec:
`docs/tech/2026-07-15-knife15-macos-m0-lease-aware-pool-selection-architecture-spec.md`

## Task 1 — Freeze Evidence And Reachability

- Record the exact M0 archive, provenance, phase results, connection sequence,
  internal invariants, exit-side immediate Connect evidence, and stop rule.
- Preserve pool=2 and every frozen Knife14/Knife15 parameter.
- Classify this change as sufficient only for the deterministic global-parity
  placement failure.

Acceptance: result document and architecture gate are complete before Rust
behavior changes.

## Task 2 — RED: Phase History Cannot Shift The Next Pair

- Add one focused behavior test through the reservation interface.
- Reserve/drop one historical flow, then keep the next two leases live.
- Require indices `0` and `1`, with observed loads `0` and `0`.
- Run only that test and preserve the expected compile/assertion RED evidence.

## Task 3 — GREEN: Atomic Least-Active Reservation

- Introduce the smallest local reservation result/module.
- Scan active lease counters in stable index order.
- CAS the observed minimum to reserve it; rescan on conflict.
- Construct the existing RAII lease from the already-reserved counter.
- Return a fail-closed error for empty or fully saturated inputs.
- Make the Task 2 tracer test green.

## Task 4 — RED/GREEN: Concurrent First Reservations

- Add a synchronized two-thread test against an idle two-slot pool.
- Require the two live reservations to occupy distinct slots.
- Drop both and require exact zero counters.
- Implement only concurrency repairs exposed by this test.

Review then exposed a second ordering requirement: reserving an active count
before awaiting a slot mutex does not stop a later same-slot opener from
overtaking the idle-exclusive probe/reconnect and cloning the old connection.
The accepted correction adds a per-slot RAII preparation gate, a focused
overtaking test, and a `Notify` waiter/wake test. The gate is held until the
connection clone is complete and is released on cancellation/error.

## Task 5 — Migrate Production Caller

- Change `live_tcp_conn` to reserve before awaiting the per-slot mutex.
- Carry `active_before` into selection diagnostics.
- Remove `tcp_next`, `tcp_pool_index`, and their round-robin test once no
  production path references them.
- Keep slot probe/reconnect decisions and primary UDP/health behavior intact.

Acceptance: focused pool selection, lease, stale probe, reconnect, and TUIC
tests pass.

## Task 6 — Diagnostic Contract

- Extend both generic and D16/native `tuic-tcp-pool-selection` lines with
  `policy=least_active active_before=<n>`.
- Add/update a focused formatting test if a stable formatter seam exists;
  otherwise assert the selection result fields and exercise both log call
  sites in review.
- Do not add high-rate periodic logs.

## Task 7 — Local Gates

Run, in order:

1. focused TUIC pool tests;
2. root `cargo test --all-targets`;
3. release build;
4. Knife15 internal/external shell self-tests and syntax;
5. Knife14 low-RTT, US-client-suite, and sing-box-control self-tests;
6. formatting and tracked/untracked diff checks.

Any unexpected repair/regression failure is analyzed before modification. The
standing user authorization permits the smallest architecture-consistent
repair without another confirmation pause; constants and acceptance SLIs stay
frozen.

## Task 8 — Code Review And Memory

- Review correctness, atomic ordering, overflow/underflow, lock ordering,
  cancellation, probe/reconnect interaction, high concurrency, throughput,
  TCP/UDP/TUN regression, and test gaps.
- Require no unresolved P0/P1.
- Update `AGENTS.md`, `HANDOFF.md`, `TODO.md`, `.learnings/LEARNINGS.md`, and
  `.learnings/ERRORS.md` with the accepted position and the invalid direct
  postmortem attempt through an auto-restored `utun` route.
- Commit coherent implementation/results changes.

## Task 9 — macOS M0 Handoff

- Rebuild the exact release binary and bind source/binary/runner hashes.
- Provide the user with baseline, direct discriminator, start/smoke/M0, and
  failure-evidence cleanup commands.
- The agent never starts macOS TUN or mutates macOS routes.
- M1 remains blocked until formal M0 and independent rearm pass.

## Completion Record

- Implementation commit: `c945a41`.
- Focused `tcp_pool_` tests: `15/15` passed.
- Root all-target tests: library `640 passed + 3 ignored`; main `2/2`.
- Release build and Clippy: PASS with only established warnings.
- Knife15 runner self-tests and Knife14 low-RTT, US-client, and sing-box
  control self-tests: PASS.
- Formatting, staged/tracked diff checks, and code review: PASS; no unresolved
  P0/P1.
- Frozen architecture, pool size, pacing constants, workload rates, and
  receiver SLI: unchanged.
