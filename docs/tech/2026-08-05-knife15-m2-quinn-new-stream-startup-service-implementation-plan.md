# Knife15 M2 Quinn New-Stream Startup Service Implementation Plan

Date: 2026-08-05

Status: **LOCAL IMPLEMENTATION COMPLETE — REAL M2 PENDING**

Architecture:
`docs/tech/2026-08-05-knife15-m2-quinn-new-stream-startup-service-architecture-spec.md`

## Task 1 — Freeze the real failure

- [x] Verify archive hash and exact source/runner/binary provenance.
- [x] Prove baseline/direct, preflights, seven cycles, same-window controls,
  Endpoint/D16 conservation, and cleanup.
- [x] Bind cycle-8 streams to installed conn1 generation 2 and record the
  exact first complete Target zero interval.
- [x] Apply the auxiliary-replacement stop rule and close tuning/repeat paths.

## Task 2 — Lock Quinn scheduling semantics

- [x] Add one deterministic proto test with an incumbent stream and one
  startup stream whose queued priority is restored atomically.
- [x] Require startup first, then ordinary same-priority fairness.
- [x] Add the minimum vendored Quinn/proto atomic write-and-restore operation.

## Task 3 — Introduce the TUIC startup-writer seam

- [x] Add one fake-writer RED for Connect-versus-business phase ownership.
- [x] Implement `arm -> write Connect -> AsyncWrite` behind one module.
- [x] Keep blocked/empty writes armed; consume on the first positive business
  write; delegate later operations unchanged.
- [x] Emit one transition diagnostic when the service is consumed.

## Task 4 — Migrate every TUIC TCP relay mode

- [x] Use the same startup writer in generic `open_tcp` and native/D16
  `open_tcp_relay`.
- [x] Preserve progress handles, exact writer pressure, leases, diagnostics,
  ordered/reassembly/native readers, and open timeout/error behavior.
- [x] Prove UDP datagram/stream paths are unchanged.

## Task 5 — Focused and regression gates

- [x] Run focused proto, Quinn, TUIC startup, pool/replacement, recovery, and
  relay-mode tests.
- [x] Run root library/binary/integration, release, established Clippy,
  Knife15/Knife14 shell, vendored Quinn/proto/docs, fmt/diff/secret.
- [x] Run exact 32MiB Endpoint capacity and require `>170 Mbit/s` with final
  conservation.

## Task 6 — Review, memory, commit, and push

- [x] Review priority lifetime, scheduler starvation, blocked/error/cancel
  behavior, lock ordering, accepted-byte reporting, every relay mode, writer
  pressure, Endpoint/pool/UDP regressions, and diagnostics.
- [x] Repair each P0/P1 with a focused test.
- [x] Write local results and update `CONTEXT.md`, `AGENTS.md`, `HANDOFF.md`,
  `TODO.md`, and `.learnings/`.
- [x] Commit coherent stages and push the current branch under standing
  authorization.

## Task 7 — One fresh real-Mac discriminator

- [ ] Pull/rebuild and run one fresh `m2-ipv6-check -> baseline ->
  direct-discriminator -> start -> smoke -> m2 -> status -> stop`.
- [ ] Preserve `status/snapshot/stop` after failure.
- [ ] Accept M2 only after the complete 24-hour schedule and cleanup pass.

Expected focused RED enters its minimum implementation. An unexpected local
regression is diagnosed against the invariant before repair; no frozen value
or SLI may be changed. A consumed startup service plus another healthy-control
installed-successor receiver zero rejects this architecture rather than
authorizing tuning.
