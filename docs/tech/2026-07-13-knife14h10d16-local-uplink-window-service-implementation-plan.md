# Knife14h10d16 Local Uplink Window Service Implementation Plan

Date: 2026-07-13
Status: **LOCAL PASS; VPS P1 PENDING**

Architecture:
`docs/tech/2026-07-13-knife14h10d16-local-uplink-window-service-architecture-spec.md`.

## Task 1 — Pin The Dependency Seam

- [x] Vendor the exact locked `smoltcp 0.10.0` source and preserve its license.
- [x] Point `[patch.crates-io]` at the vendored source without changing version
  or features.
- [x] Prove a clean dependency build before behavior changes.

## Task 2 — Write The Protocol RED

- [x] Add a focused smoltcp test with `1 MiB` storage and `368,640B` window
  limit.
- [x] Prove the current code exposes/accepts the full physical window.
- [x] Cover SYN advertisement, established updates, and segment acceptance.

Expected RED authorizes the corresponding minimal implementation.

## Task 3 — Add The Window-Limit Primitive

- [x] Add an optional receive-window limit to the smoltcp TCP socket.
- [x] Apply it to scaled/unscaled advertisement and acceptance-window end.
- [x] Preserve default behavior, reset/relisten, zero-window, and window scale.
- [x] Add public getter/setter documentation and focused upstream-style tests.

## Task 4 — Wire The Fixed H10d16 Policy

- [x] Derive `368,640B` from the fixed EndpointWindowV1 rate and burst.
- [x] Extend listener configuration with an optional local uplink window.
- [x] Install it before H10d16 sockets listen; leave non-H10 unchanged.
- [x] Print storage and advertised-window attribution at startup.
- [x] Add no environment override and change no frozen existing constant.

## Task 5 — Add Reachability And Observability Tests

- [x] Prove H10 listener storage stays `1 MiB` while its limit is `368,640B`.
- [x] Prove default listeners retain physical-window behavior.
- [x] Record receive-queue high-water and configured window in aggregate
  diagnostics without per-packet logging.
- [x] Preserve permit-before-dequeue and relay lifecycle tests.

## Task 6 — Run Local Gates

- [x] Focused smoltcp window tests.
- [x] Explicit vendored-smoltcp suite for the enabled feature set.
- [x] Focused mini_vpn TCP/TUN/D16/EndpointWindow tests.
- [x] Exact `32 MiB` real-Quinn local forward harness: `>170 Mbit/s`, zero
  drops/full waits, queue high-water below `500`, exact bytes and clean EOF.
- [x] Root unit/integration/concurrency/UDP gates.
- [x] Vendored Quinn/Quinn-proto tests and docs.
- [x] `cargo fmt --check`, `cargo check`, script syntax, and diff checks.

Stop rather than tune if the exact local gate is `<=170 Mbit/s` or an
unexpected repair/regression test fails.

## Task 7 — Review And Commit

- [x] Review TCP acceptability, scaling, reset, default-path compatibility,
  hot-path cost, boundedness, fairness, lifecycle, and missing-test risk.
- [x] Fix P0/P1 findings and rerun affected gates.
- [x] Update `.learnings/LEARNINGS.md`; update `.learnings/ERRORS.md` for any
  failure that changes future behavior.
- [x] Commit one coherent implementation stage without secrets or artifacts.

## Task 8 — One Frozen VPS P1

- [ ] Deploy an exact, secret-free source snapshot and verify hashes.
- [ ] Rehearse profile/preflight without iperf.
- [ ] Run one target-only forward P1 with all frozen inputs.
- [ ] Require receiver `>170 Mbit/s`, zero TUN drops, pump below `500/500`,
  zero full waits, QUIC loss `<=16 MiB`, exact endpoint conservation, and
  clean lifecycle/cleanup.
- [ ] Stop the branch on failure; do not tune the window or any frozen value.

## Task 9 — Archive The Result

- [ ] Store a sanitized evidence bundle and checksum.
- [ ] Update result doc, `HANDOFF.md`, `TODO.md`, `AGENTS.md`, and project
  learning memory with the exact accepted or failed position.
- [ ] Commit the evidence stage.
