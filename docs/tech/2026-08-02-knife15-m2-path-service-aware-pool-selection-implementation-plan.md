# Knife15 M2 Path-Service-Aware TCP Pool Selection Implementation Plan

Date: 2026-08-02

Status: **COMPLETE — fresh real M2 acceptance pending; M3 blocked**

Source spec:
`docs/tech/2026-08-02-knife15-m2-path-service-aware-pool-selection-architecture-spec.md`

## Task 1 — Freeze Bundle Evidence And Stop Branch

- Bind the exact archive hash, start source, direct discriminator, failed M2
  phase, exact network controls, QUIC stats, D16 wait, Endpoint conservation,
  and cleanup outcome.
- Reject partial-tail, operator, general path-outage, TUN, ownership, and dead
  connection branches.
- Preserve every frozen architecture and M2 SLI.

Acceptance: the architecture spec is complete before Rust behavior changes.

## Task 2 — RED: Equal Busy Loads Need Path Service

- Add one focused selector test with both slots at the observed nonzero load.
- Supply the observed `10,124B/163ms` and `23,842B/163ms` samples.
- Require slot 1 and require that diagnostics report a path-service tie-break.
- Run only the focused test and preserve the expected assertion RED.

## Task 3 — GREEN: Plain Path-Service Ordering

- Add a small plain-value service sample independent of Quinn types.
- Compare known samples by exact `cwnd/RTT` cross multiplication in `u128`.
- Make service a tie-break only after equal nonzero active loads.
- Keep stable index ordering for idle, unknown, and equal-service cases.
- Carry the selected sample and tie-break classification in the reservation.

Acceptance: Task 2 becomes green with no production integration yet.

## Task 4 — RED/GREEN: Preserve Accepted Ordering

- Add focused tests for idle `0 -> 1`, unequal-load precedence, known versus
  unknown, equal service, and all unknown.
- Re-run existing simultaneous reservation, preparation overtaking, waiter,
  saturation, and lease clone/drop tests.
- Repair only invariant violations introduced by the deepening.

## Task 5 — Quinn Adapter And Production Migration

- In `live_tcp_conn`, non-blockingly sample each current slot with `try_lock`.
- Convert Quinn `PathStats` to the plain service sample; closed, locked,
  zero-window, or zero-RTT slots become unknown.
- Pass the complete sample vector into the selector before reservation.
- Do not hold a slot mutex while sampling another or while reserving.
- Keep all existing probe/reconnect/clone and error paths intact.

## Task 6 — Diagnostic Contract

- Rename the selected policy to `least_active_then_path_service`.
- Emit selected `path_cwnd`, `path_rtt_us`, and
  `path_service_tiebreak=true|false`.
- Update the focused formatter test and confirm Knife14 parsing ignores the
  additive fields.
- Add no periodic or payload-hot-path logging.

## Task 7 — Focused And Full Local Gates

Run in risk order:

1. focused new selector tests;
2. all `tcp_pool_` and TUIC recovery tests;
3. root `cargo test --all-targets --features harness`;
4. integration harness, release build, and Clippy;
5. Knife15 internal/external self-tests and Bash syntax;
6. Knife14 low-RTT, US-client, and sing-box-control self-tests;
7. vendored Quinn/quinn-proto unit and doc tests;
8. formatting, diff, tracked/untracked, and secret scans.

Unexpected regression failures are diagnosed before repair. No constant or
SLI changes are allowed.

## Task 8 — Code Review

Review:

- comparison correctness and `u128` overflow safety;
- atomic selection races and stale point-in-time samples;
- lock ordering, cancellation, waiter wake-up, and saturation;
- idle-pair determinism and primary UDP/health ownership;
- open/probe/reconnect generation behavior;
- TCP, UDP, TUN, D16, Endpoint, high-concurrency, and throughput regression;
- diagnostic compatibility and missing tests.

Acceptance: no unresolved P0/P1.

## Task 9 — Results And Project Memory

- Write one local-results document with RED/GREEN and every gate outcome.
- Update `AGENTS.md`, `HANDOFF.md`, `TODO.md`, `.learnings/LEARNINGS.md`, and
  `.learnings/ERRORS.md` with the accepted M2 failure, cleanup success, repair,
  and next stop position.
- Commit one coherent change and push the current branch under the standing
  repository rule.

## Task 10 — Fresh M2 Handoff

- Require a descendant containing the reviewed repair and a rebuilt release
  binary.
- Give the user the exact fresh HK
  `IPv6-off -> baseline -> direct -> start -> smoke -> m2 -> status -> stop`
  sequence.
- Do not repeat the failed bundle or tune M2 values.
- Keep M3 blocked until full M2 and cleanup acceptance pass.

Completion result:
`docs/tech/2026-08-02-knife15-m2-path-service-aware-pool-selection-local-results.md`.
