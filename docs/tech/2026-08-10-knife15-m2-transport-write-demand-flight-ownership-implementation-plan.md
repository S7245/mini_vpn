# Knife15 M2 Transport-Write-Demand Flight Ownership Implementation Plan

Date: 2026-08-10

Architecture:
`docs/tech/2026-08-10-knife15-m2-transport-write-demand-flight-ownership-architecture-spec.md`.

## Task 1 — Preserve paired evidence

- [x] Verify the Mac SHA-256 and exact source `b437b93`.
- [x] Stop and bundle the exact overlapping `.33` observer.
- [x] Identify the first failed phase and validate complete cleanup.
- [x] Align the business socket's Exit supply and Target ACK timing.

## Task 2 — Reject frozen and downstream branches

- [x] Validate D16 queue/lease/reservation ownership and writer ACK progress.
- [x] Validate TUN/interface, Endpoint conservation, routes, and process.
- [x] Prove Exit supply has no multi-second gap and Target ACKs immediately.
- [x] Select client-to-Exit QUIC supply ownership without tuning constants.

## Task 3 — Reproduce the production interleaving

- [x] Use the realistic Quinn pair and production Endpoint policy.
- [x] Deliver Writable and make one successful partial retry.
- [x] Run the connection worker before the writer's next Blocked retry.
- [x] Observe RED `24,000 -> 1,772,034B`, below `1,990,080B`.

## Task 4 — Own the exact accepted flight

- [x] Retain the existing per-stream Blocked-retry bit.
- [x] Add one exclusive per-stream demand offset boundary.
- [x] Extend the boundary on the first nonempty retry after Blocked.
- [x] Tag only STREAM ranges below the exact boundary.
- [x] Clear only after cumulative ACK or terminal lifecycle.

## Task 5 — Lock down isolation and lifecycle

- [x] Preserve old Writable-before-retry behavior.
- [x] Prove reset of one stream preserves another stream's ownership.
- [x] Prove a deferred writer cannot lend ownership across streams.
- [x] Prove later same-stream bytes cannot inherit a completed boundary.
- [x] Run complete finish/reset/STOP/0-RTT suites.

## Task 6 — Complete gates and review

- [x] Quinn-proto `325/325`, docs `3/3`, and default-feature Clippy.
- [x] Quinn adapter `40+3 ignored`, integration expected ignored, doc `1`.
- [x] Root `689+3 ignored`, main `2`, integration `10+4 ignored`.
- [x] Release, established Clippy, shell, root docs, and exact 32MiB capacity.
- [x] Complete final narrow diff/vendor/secret review after documentation.
- [x] Record no unresolved P0/P1 and update project memory.

## Task 7 — Commit, push, and paired qualification

- [x] Commit coherent implementation/evidence changes (`c218d8a`) and push.
- [ ] Start one fresh bounded `.33` observer only when the Mac is ready.
- [ ] Take exactly one `m2-ipv6-check -> baseline -> direct-discriminator ->
  start -> smoke -> m2-qualification -> status -> stop`.
- [ ] Preserve failure evidence; do not run formal M2 or tune/repeat
  unchanged.
