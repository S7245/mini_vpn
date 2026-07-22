# Knife15 Quinn Multi-Connection Rebind Retention Implementation Plan

Date: 2026-07-21

Status: **COMPLETE — LOCAL TDD, REGRESSION, REVIEW, AND HANDOFF PASS**

Architecture:
`2026-07-21-knife15-quinn-multi-connection-rebind-retention-architecture-spec.md`.

Source baseline: `1c587ba8bcc4c78c7fb77de72c1d40aba87f88f5`.

## Task 1: Lock Quinn's multi-connection retention behavior

- [x] Add one focused RED at the Quinn Endpoint seam proving that current-
  socket recovery of connection A cannot release connection B's old receive
  path.
- [x] Add the smallest pending-handle state module required by the test.
- [x] Keep allocation at rebind time, never per packet.
- [x] Run only the focused Quinn test and record RED/GREEN evidence.

## Task 2: Carry socket origin to each connection

- [x] Add a current-socket generation tag to Endpoint-to-Connection packet
  events; old-socket events carry no generation.
- [x] Store one monotonic generation in Quinn `Connection` state and expose a
  read-only method.
- [x] Add focused tests for current advance and old-socket non-advance.
- [x] Verify ordinary non-rebind generation `0` behavior is unchanged.

## Task 3: Close old-socket lifecycle races

- [x] Remove a handle from the pending set on Endpoint drain.
- [x] Prove all recovered/drained entries release the previous socket.
- [x] Prove a connection inserted after rebind cannot extend the old socket.
- [x] Prove a second explicit rebind replaces, rather than chains, previous
  socket ownership.

## Task 4: Require all-connection proof in mini_vpn

- [x] Add one pure fake-time RED where Endpoint-level generation advances and
  only conn0 has per-connection generation `g`; expect no `Recovered`.
- [x] Snapshot stable IDs on successful rebind and require all to reach `g`.
- [x] Keep missing/replaced unrecovered identities fail-closed.
- [x] Extend the recovery log with recovered/expected connection counts
  without changing runner-compatible fields.

## Task 5: Exercise the real two-connection seam

- [x] Extend the existing `EndpointWindowV1` loopback rebind test to require
  per-connection generation `1` on both established connections.
- [x] Preserve both post-rebind round trips and exact Endpoint conservation.
- [x] Run focused Quinn, mini_vpn policy, and loopback integration tests.

## Task 6: Run regression and capacity gates

- [x] Root library and binary tests.
- [x] All-target check, release build, and project Clippy gate.
- [x] Vendored Quinn and Quinn-proto unit/doc gates using the local patches.
- [x] Knife15 runner/wrapper and Knife14 harness self-tests.
- [x] Rust fmt, shell syntax, diff, and secret checks.
- [x] Re-run the local `32MiB >170 Mbit/s` EndpointWindowV1 gate if the Quinn
  receive/rebind changes touch its normal hot path. Failure is an architecture
  failure; do not tune constants.

## Task 7: Review, memory, and handoff

- [x] Use `code-review` for correctness, performance, lifecycle, bounded state,
  TUN/UDP/TCP regression, and missing-test risk.
- [x] Repair every P0/P1 and rerun affected gates.
- [x] Record the failed M1 and completed local repair in a results document.
- [x] Update `HANDOFF.md`, `TODO.md`, `.learnings/LEARNINGS.md`, and
  `.learnings/ERRORS.md`.
- [x] Commit one coherent change and push the current branch.
- [x] Only then provide a fresh user-run HK M1 workflow. M2/M3 stay blocked.

## Stop Rules

- Expected tracer-bullet RED may enter its minimum GREEN.
- Unexpected repair/regression failures require causal analysis before edits.
- Do not weaken receiver or UDP SLOs and do not tune frozen constants.
- If all-connection retention cannot preserve EndpointWindowV1 or both live
  loopback connections, reject the architecture.
- If all local gates and review pass, the next real M1 is authorized without a
  separate approval; the user still executes every macOS TUN/sudo command.
