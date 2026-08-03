# Knife15 M2 Busy-Epoch Forward Qualification Implementation Plan

Date: 2026-08-03

Status: **LOCAL IMPLEMENTATION COMPLETE — Task 10 real acceptance pending**

Source spec:
`docs/tech/2026-08-03-knife15-m2-busy-epoch-forward-qualification-architecture-spec.md`

## Task 1 — Freeze Both Bundle Roles

- Bind the Wi-Fi external-path failure only as a clean-short comparator.
- Bind the Ethernet baseline/direct, exact receiver-zero rows, same-window
  controls, open placement, Quinn stats, writer wait, conservation, and
  cleanup.
- Record why ordinary congestion events and raw `cwnd/RTT` cannot be hard
  qualification facts.

Acceptance: architecture spec complete before Rust behavior changes.

## Task 2 — RED/GREEN: Busy-Epoch Qualification Tracer

- Add one focused admission test that first establishes black-hole anchors at
  exact idle ownership.
- Set both slots busy at the observed loads and advance only conn1 from zero to
  ten black-hole detections.
- Supply observed path service and require conn0 despite conn1's lower lease
  load and higher instantaneous service.
- Capture RED, then add the minimum per-slot qualification state and candidate
  isolation required for GREEN.

## Task 3 — RED/GREEN: Idle Recovery And Availability

- Prove active zero anchors the current counter and preserves stable
  `conn0 -> conn1` startup ordering.
- Prove all-degraded fallback remains least-active, then path-service, then
  stable index.
- Prove missing observation and counter regression are unknown rather than
  degraded or qualified.
- Prove busy identity replacement cannot reuse a stale anchor.

## Task 4 — Preserve Existing Admission Invariants

- Keep preparing/saturation/empty errors and atomic CAS behavior unchanged.
- Commit an idle epoch anchor only after the selected slot wins reservation;
  a losing stale idle observation must not rewrite a live busy epoch.
- Keep waiter resampling after preparation wake-up.
- Keep simultaneous idle reservations distinct.
- Keep lease clone/drop conservation and stale auxiliary liveness behavior.
- Preserve prior equal-busy path-service behavior inside the admitted set.

## Task 5 — Quinn Adapter And Open Migration

- Convert each current slot to the plain identity/path/black-hole observation
  with existing `try_lock`.
- Pass the complete observations into the deepened admission module.
- Keep slot mutex/probe/reconnect/connection-clone ordering unchanged.
- Do not add writer, packet, pacing, or Endpoint hot-path work.

## Task 6 — Diagnostic Contract

- Keep the existing selection prefix and generation/probe/reconnect fields.
- Add policy, selected qualification, black-hole anchor/current,
  qualification override, all-degraded fallback, and both candidate summaries.
- Keep additive compatibility with Knife14 parsers.
- Add a focused formatter test and no periodic/payload logging.

## Task 7 — Focused And Full Gates

Run in risk order:

1. exact new admission tests;
2. all `tcp_pool_` and TUIC recovery tests;
3. root `cargo test --all-targets --features harness`;
4. main/integration, release, and Clippy;
5. Knife15 internal/external shell tests and Bash syntax;
6. Knife14 low-RTT, US-client, and sing-box-control self-tests;
7. vendored Quinn/quinn-proto unit and doc tests;
8. 32 MiB `>170 Mbit/s` Endpoint capacity gate;
9. fmt, diff, worktree, link, and changed-content secret checks.

Unexpected failures are classified before repair. No frozen constant, SLO, or
test load may change.

## Task 8 — Code Review

Review:

- identity/counter monotonicity and active-zero epoch semantics;
- concurrent open races, mutex/CAS ordering, wait/cancel, and poison handling;
- degraded isolation versus all-degraded availability;
- primary UDP/health ownership and active-flow noninterference;
- old path-service, probe, reconnect, and generation behavior;
- TCP/UDP/TUN/D16/Endpoint/high-concurrency/throughput regressions;
- diagnostic completeness and parser compatibility;
- missing deterministic tests.

Acceptance: no unresolved P0/P1.

## Task 9 — Results, Memory, Commit, Push

- Write local results with RED/GREEN and every gate.
- Update `AGENTS.md`, `HANDOFF.md`, `TODO.md`, `CONTEXT.md`, and learning/error
  memory with both bundle classifications and the exact next stop.
- Commit coherent tasks without secrets and push the current branch under the
  standing authorization.

## Task 10 — Fresh M2 Handoff

- Require the pushed reviewed descendant and rebuilt release binary.
- Give exactly one fresh user-run
  `m2-ipv6-check -> baseline -> direct -> start -> smoke -> m2 -> status -> stop`.
- Preserve failure evidence; do not tune or repeat unchanged.
- Keep M3 blocked until full M2 and cleanup acceptance.
