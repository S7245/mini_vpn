# Knife15 M2 Transport-Write-Demand Ownership Implementation Plan

Date: 2026-08-08

Architecture:
`docs/tech/2026-08-08-knife15-m2-transport-write-demand-architecture-spec.md`.

## Task 1 — Preserve the artifact boundary

- [x] Verify SHA-256 and exact source `eb2185f`.
- [x] Identify the first failed phase and validate cleanup.
- [x] Compare client writer/ACK bytes with Target bytes.
- [x] Record that no fresh paired Exit observer existed.

## Task 2 — Build the writer-demand RED

- [x] Use a real Quinn pair with `82ms` one-way latency.
- [x] Begin business service after a successful successor turn.
- [x] Force repeated send-window `WriteError::Blocked` and Writable delivery.
- [x] Observe old cwnd growth `24,000 -> 151,200B` after `2MiB`.

## Task 3 — Own demand per stream

- [x] Add one boolean to each existing send stream.
- [x] Add one O(1) connection aggregate count.
- [x] Keep ownership across Writable event delivery.
- [x] Clear on successful nonempty retry and terminal paths.
- [x] Tag only packets that actually carry a demand-owning stream's frame.

## Task 4 — Lock down multi-stream lifecycle

- [x] Prove resetting one blocked stream does not erase another stream's
  demand.
- [x] RED/GREEN a cancelled/deferred writer against an independent stream.
- [x] Run the complete stream and connection suites for finish/reset/STOP and
  0-RTT regressions.

## Task 5 — Separate MTUD from readiness

- [x] Turn the adopted-probe test RED for independent ownership.
- [x] Exclude only the exact active preexisting PLPMTUD probe.
- [x] Keep authentication STREAM adoption unchanged.
- [x] Prove MTUD still records probe loss while the service turn succeeds.

## Task 6 — Complete gates and reverse review

- [x] Run focused and complete Quinn/proto tests (`323/323`, docs `3/3`).
- [x] Run root, integration, release, Clippy, shell, docs, formatting,
  vendor, secret, and exact 32MiB capacity gates.
- [x] Review counter lifecycle, multi-stream safety, hot-path cost, loss/path
  behavior, and all frozen paths.
- [x] Update PATCHES; update project learning/error memory with final results.

## Task 7 — Commit, push, and paired qualification

- [x] Commit coherent implementation and evidence changes (`f9c3c23`).
- [x] Push the reviewed descendant (`2dec9ec`).
- [ ] Start a fresh bounded `.33` observer only when the Mac is ready.
- [ ] Take exactly one `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2-qualification -> status -> stop`.
- [ ] Preserve failure evidence; do not run formal M2 or tune/repeat
  unchanged.
