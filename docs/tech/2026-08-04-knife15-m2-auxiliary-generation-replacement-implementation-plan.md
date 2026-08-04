# Knife15 M2 Auxiliary Generation Replacement Implementation Plan

Date: 2026-08-04

Status: **READY FOR LOCAL TDD**

Architecture:
`docs/tech/2026-08-04-knife15-m2-auxiliary-generation-replacement-architecture-spec.md`

## Task 1 - Freeze the failure replay and formal UDP fail-fast

- Add a shell fixture where formal UDP loss `3.000001%` is rejected at the
  phase and exact `3.0%` passes.
- Keep diagnostic continuation recording unchanged.
- Persist failure reason, value, limit, and evidence path.
- Run Knife15 internal/external self-tests and syntax.

## Task 2 - Introduce generation-bound lease ownership

- Add a per-generation active counter plus zero notification.
- Make each lease update generation active and pool-wide active total.
- Preserve clone/drop and saturation semantics.
- RED/GREEN zero notification and cross-generation isolation.

## Task 3 - Separate admission decision from generation preparation

- Extend the plain admission outcome with `ReserveCurrent` versus
  `ReplaceAuxiliary`.
- Replay qualified conn0 `active14` and degraded conn1 `active6`.
- Require replacement rather than another conn0 reservation.
- Preserve primary-only degradation and ordinary qualified ordering.

## Task 4 - Extract the TCP pool generation module

- Introduce one parameter object for connection, identity/generation,
  generation lease counter, write pressure, and auth/open evidence.
- Move current auxiliary slot state behind the module interface.
- Keep the production Quinn adapter and add a deterministic in-memory adapter.
- Use Parallel Change; production behavior stays unchanged in this task.

## Task 5 - Implement single-owner successor preparation

- Reuse the existing bounded handshake/auth path.
- Hold replacement preparation authority without holding a Quinn or endpoint
  lock across await.
- Verify predecessor identity/generation before install.
- RED/GREEN concurrent opens and stale handshake completion.

## Task 6 - Atomically install and reserve the successor

- Swap current generation, write pressure, and active counter under one slot
  authority.
- Commit successor qualification anchor only with the first reservation.
- Bind the triggering open to the successor.
- Prove no opener can reserve the predecessor after install.

## Task 7 - Drain and reap the predecessor

- Retain predecessor connection and pressure evidence while generation active
  is nonzero.
- On exact zero, close only the predecessor and emit drain evidence.
- Bound each auxiliary slot to one predecessor and the frozen pool to one
  draining predecessor globally.
- RED/GREEN long-lived predecessor and replacement-blocked cases.

## Task 8 - Preserve endpoint recovery and diagnostics by identity

- Sample current and draining generations independently.
- Attribute writer pressure to its exact stable id/generation.
- Keep endpoint-wide socket rebind semantics unchanged.
- Label QUIC stats and selection/replacement/drain transitions.

## Task 9 - Migrate both TCP open interfaces

- Make generic and D16 native opens consume the same acquired generation.
- Return connection, generation-bound lease, write pressure, and diagnostics
  as one selection object.
- Remove parallel-vector lookups from both callers.
- Preserve TUIC Connect bytes and all relay modes.

## Task 10 - Failure and lifecycle regressions

- Handshake timeout/failure leaves current state intact.
- Primary conn0 is never generation-replaced.
- UDP send, heartbeat, health probe, and primary reconnect remain conn0.
- Existing predecessor streams receive no reset during successor install.
- Drop/cancel after reservation releases every count and preparation token.

## Task 11 - Local gates and code review

- Focused pool/replacement and shell RED/GREEN tests.
- Root library/binary/integration, release, and all-target Clippy.
- Knife15 internal/external runner and Knife14 shell gates.
- Vendored Quinn and patched quinn-proto tests/docs.
- Endpoint 32MiB capacity `>170 Mbit/s` and final conservation.
- fmt, diff check, secret scan, and P0/P1 code review.

## Task 12 - Memory, commit, push, and real-Mac handoff

- Record results in a local-results document.
- Update `AGENTS.md`, `HANDOFF.md`, `TODO.md`, and learnings/errors.
- Commit coherent runner and pool stages, then push the current branch under
  the user's standing authorization.
- Request exactly one fresh M2 transaction; do not repeat unchanged or tune.
