# Knife15 M2 Quinn Successor Service Turn Implementation Plan

Date: 2026-08-07

Status: **LOCAL IMPLEMENTATION AND REVIEW COMPLETE; RESULTS/MEMORY/DELIVERY IN PROGRESS; FORMAL M2 AND M3 REMAIN BLOCKED**

Source of truth:
`docs/tech/2026-08-07-knife15-m2-quinn-successor-service-turn-architecture-spec.md`.

## Task 1: Preserve And Classify Evidence

- [x] Verify artifact checksum, exact source, direct controls, phase failure,
  routes, process, TUN, D16, Endpoint conservation, and cleanup.
- [x] Align control/data admission, generation provenance, writer progress,
  path service, and receiver-zero timing.
- [x] Reject operator, network-control, Target, TUN, pacing, D16, selector,
  parameter-tuning, and close-tail branches.
- [x] Record that the intended paired Exit observer expired before the run and
  bound all resulting inference accordingly.

## Task 2: Freeze The Architecture

- [x] Select one ACK-correlated current-cwnd Quinn service turn before
  successor install.
- [x] Define lifecycle, byte, ACK/loss, path-generation, Endpoint pacing, CAS,
  observability, capacity, and stop invariants.
- [x] Preserve every frozen value and reject retry, Target probe, extra warm
  generation, and static readiness-threshold branches.

## Task 3: Focused RED

- [x] Compose existing generation-slot CAS/current-ownership tests with the
  new pre-install service await and fail-closed transport outcomes.
- [x] Add deterministic quinn-proto tests for one bounded tagged flight,
  ACK-only completion, loss terminality, and path-generation mismatch.
- [x] Add Quinn adapter tests for one owner and exact result, plus protocol
  close/path/loss terminal coverage and deadline partial snapshots.
- [x] Add Endpoint bulk-class and conservation coverage for service-turn
  datagrams.

## Task 4: Minimal GREEN

- [x] Add the private quinn-proto service-turn state and packet tag.
- [x] Emit valid congestion-controlled PING+PADDING packets until the exact
  snapshotted flight is covered.
- [x] Complete the state only from tagged ACK/loss/close/path-change events.
- [x] Expose one hidden Quinn async adapter and sequence it between successor
  authentication and existing generation CAS installation.
- [x] Add exact bounded diagnostics without secrets or payload.

## Task 5: Gates And Review

- [x] Run focused replacement, admission, recovery, proto, Quinn, and Endpoint
  tests.
- [x] Run the exact 32MiB Endpoint capacity/conservation gate.
- [x] Run root library/main/integration/release/Clippy, Knife15/Knife14/Exit
  shell, vendored Quinn/proto/docs, root docs, fmt/diff/vendor/secret gates.
- [x] Review concurrency, cancellation, late ACK, path migration, loss, close,
  CAS, Endpoint debt, TCP/UDP/TUN/D16 regression, and operational evidence.
- [x] Repair every P0/P1 finding and rerun affected gates.

## Task 6: Results, Memory, Commit, Push

- [ ] Write local results with exact tests and capacity.
- [ ] Update `CONTEXT.md`, `TODO.md`, `HANDOFF.md`, `AGENTS.md`, learnings, and
  errors.
- [ ] Commit coherent stages and push the current branch.
- [ ] Start a fresh paired Exit observer and provide exactly one bounded Mac
  qualification transaction. Formal M2 remains blocked.

## Stop Rule

Expected focused RED may enter minimal GREEN. Stop on a new knob/retry, Target
or application probe, pool expansion, frozen-value change, unbounded state,
local 32MiB capacity at or below `170 Mbit/s`, or unexpected regression. A
failed Mac qualification must be classified with exact service-turn,
generation, writer, and paired Exit evidence before another architecture.
