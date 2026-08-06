# Knife15 M2 Connection-Local Path-State Recovery Implementation Plan

Date: 2026-08-06

Status: **AUTHORIZED AND IN PROGRESS; FORMAL M2 REMAINS BLOCKED**

Source of truth:
`docs/tech/2026-08-06-knife15-m2-connection-local-path-state-recovery-architecture-spec.md`.

Goal: add the smallest one-shot connection-local Quinn path-state recovery
that handles exact writer pressure plus same-window PLPMTUD black-hole growth,
without changing any frozen value or replaying application payload.

## Task 1: Preserve Exact Evidence

- [x] Verify the Mac artifact SHA/source/binary/runner provenance.
- [x] Verify baseline/direct, start/smoke, qualification, transfer completion,
  Endpoint/D16/TUN, and cleanup evidence.
- [x] Stop, bundle, verify, and inspect the paired Exit observer.
- [x] Correlate exact Target TCP_INFO/pcap with Mac sender intervals and
  per-connection Quinn counters.
- [x] Classify client-to-Exit connection-local QUIC service degradation and
  reject operator, Exit kernel/Target, TUN, pacing, and parameter branches.

## Task 2: Freeze Architecture And Vocabulary

- [x] Write goals, non-goals, capacity, exact path, old-path audit,
  invariants, TDD, failure discriminators, and stop rule.
- [x] Select one-shot same-connection path-state reset; reject shared socket
  rebind broadening, cross-connection stream replay, and another selector.
- [x] Add the connection-local recovery term to `CONTEXT.md`.

## Task 3: Pure Policy RED/GREEN

- [x] Extend connection samples/counters with exact
  `black_holes_detected`.
- [x] RED ACK-progress + Pending + black-hole advance -> one path reset.
- [x] RED no-advance, no-Pending, counter-regression, healthy-peer isolation,
  one-shot identity, replacement identity, and hard-stall precedence cases.
- [x] GREEN with per-identity pending-window anchors and consumed authority.
- [x] Preserve generic UDP no-RX and exact TCP ACK-stall rebind behavior.

## Task 4: Quinn Mechanism Adapter

- [x] RED a real same-connection/same-stream reset-and-continue test.
- [x] Expose a hidden high-level Quinn `path_changed` adapter that calls the
  existing proto mechanism using runtime time and wakes the driver.
- [x] Update the pinned Quinn patch manifest/checksum evidence.
- [x] Run focused Quinn/proto tests.

## Task 5: TUIC Application And Observation

- [x] Capture the selected stable identity and exact handle across current/draining pool
  generations without payload replay or lock inversion.
- [x] Apply only to that Quinn connection; consume authority on issued action
  even for identity disappearance/closure.
- [x] Log exact trigger, bound, black-hole anchor/current, generation, and
  apply result without secrets.
- [x] Add focused application/lookup tests where the existing seams permit.

## Task 6: Focused And Capacity Gates

- [x] Run all Endpoint recovery, TCP pool qualification/replacement, writer
  pressure, startup service, and QUIC adapter tests.
- [x] Run the exact `32 MiB` Endpoint gate and require `>170 Mbit/s`, exact
  bytes/EOF, zero socket would-block, and final `61,440/0/0B` conservation.
- [x] Stop on capacity failure; do not tune constants.

## Task 7: Full Gates And Review

- [x] Root library/main/integration/release/Clippy gates.
- [x] Knife15/Knife14 shell syntax and self-tests.
- [x] Vendored Quinn/proto unit and doc tests.
- [x] fmt, diff, patch manifest, and secret checks.
- [x] Review correctness, boundedness, lock ordering, one-shot authority,
  connection replacement, ACK-stall precedence, UDP/TCP/TUN/D16/Endpoint
  regressions, observability, and missing tests.
- [x] Repair all P0/P1 findings and rerun affected gates.

## Task 8: Results, Memory, Commit, Push

- [x] Write local result and exact gate counts/rates.
- [x] Update `TODO.md`, `HANDOFF.md`, `AGENTS.md`, learnings, and errors.
- [x] Commit coherent spec/implementation/result stages and push the current
  branch.
- [x] Provide one exact Mac `m2-qualification` transaction; do not run formal
  M2.

## Stop Rule

Expected TDD RED may enter the corresponding minimal GREEN. Unexpected
repair/regression failures are diagnosed against the architecture and fixed
in scope under the user's standing authorization. Frozen-value changes,
payload replay, repeated reset authority, or formal M2 expansion are not
authorized by this plan.
