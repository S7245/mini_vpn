# Knife15 M2 Successor Forward-Service Inheritance Implementation Plan

Date: 2026-08-10

Architecture:
`docs/tech/2026-08-10-knife15-m2-successor-forward-service-inheritance-architecture-spec.md`.

## Task 1 — Preserve and classify formal evidence

- [x] Verify archive hash, source, binary, runner, preflight, and cleanup.
- [x] Align the exact failed phase with selection, D16, recovery, QUIC, and
  network evidence.
- [x] Reject operator, network, Target, TUN, Endpoint, resource, and cleanup
  causes supported by the artifact.
- [x] Apply the previous stop rule and reject one-turn certification as
  sufficient.

## Task 2 — Freeze the inheritance architecture

- [x] Define exact generation ownership and monotonic dynamic floor state.
- [x] Reuse sequential exact service turns and the existing replacement
  deadline; reject new constants, retries, or background work.
- [x] Define path-reset ordering, fail-closed behavior, capacity math, hot
  paths, old-path audit, and failure discriminators.
- [x] Preserve all frozen data-plane and workload values.

## Task 3 — RED generation-owned floor

- [x] Require the exact current identity to publish/read a positive floor.
- [x] Prove repeated lower samples cannot reduce it and stale identities
  cannot mutate it.
- [x] Prove reconnect invalidates prior transport-owned evidence.

## Task 4 — GREEN minimal scalar state

- [x] Add the floor beside `TcpPoolGeneration` certificate/lifecycle state.
- [x] Add narrow slot-owned read/reset/install checks with exact
  identity/generation validation.
- [x] Record and reset atomically; skip reset when exact ownership is stale.
- [x] Seed installed successors from their final proved certificate floor and
  recheck it against the latest predecessor floor inside install CAS.

## Task 5 — RED/GREEN inherited transport proof

- [x] Add a real Quinn-pair RED whose dynamic floor requires more than one
  current-cwnd service turn.
- [x] Add one private proof helper that aggregates sequential exact turns on
  one path within the existing deadline.
- [x] Fail on loss, path change, close, timeout, or no cwnd progress below the
  floor; preserve one-turn compatibility without a floor.
- [x] Extend replacement/install diagnostics with requirement, rounds,
  aggregate bytes, and final floor.

## Task 6 — Lifecycle and regression gates

- [x] Lock down replacement CAS, predecessor drain, attempt-local fallback,
  initial/Unknown availability, Ready anti-churn, and stale-certificate paths.
- [x] Run root, main, integration, release, established Clippy, shell, docs,
  vendored Quinn/proto, fmt, diff, vendor, secret, and exact capacity gates.

## Task 7 — Review, memory, commit, and qualification

- [x] Review correctness, concurrency, performance, boundedness, D16,
  Endpoint, TUN, TCP/UDP, lifecycle, and missing tests; resolve all P0/P1.
- [ ] Update `CONTEXT.md`, `.learnings`, `HANDOFF.md`, and `TODO.md`.
- [ ] Commit one coherent implementation/evidence stage and push.
- [ ] Run exactly one fresh paired Mac qualification before formal M2.
- [ ] Do not repeat or tune unchanged; a recurrence after a nonzero inherited
  floor rejects the mechanism as sufficient.
