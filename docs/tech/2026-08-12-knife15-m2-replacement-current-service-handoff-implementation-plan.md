# Knife15 M2 Replacement Current-Service Handoff Implementation Plan

Date: 2026-08-12

Status: **TASKS 1-7 COMPLETE; PAIRED MAC QUALIFICATION NEXT**

Architecture:
`docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-architecture-spec.md`.

## Task 1: Lock the failing behavior with one RED

Add one real Quinn-pair test through `TcpPoolGenerationSlot<Connection>`.
Create an exact generation with an older positive floor, grow the same
connection's current cwnd above it through ACK-owned service, prepare a
replacement, and require the returned handoff to expose and publish the exact
higher current cwnd. Confirm the test fails for the expected missing interface
or stale-floor behavior, not for harness setup.

## Task 2: Add the deep handoff interface

Introduce a small private handoff value containing `observed_cwnd` and
`required_cwnd_floor`. Add one slot method that holds the mutex across exact
identity/generation validation, Quinn stats sampling, positive validation, and
monotonic floor publication. Keep the existing scalar getter test-only.

Run the Task 1 test GREEN.

## Task 3: Route production replacement through the handoff

Replace the read-only floor lookup in `replace_auxiliary_generation()` with
the new preparation operation. Pass the required floor to the unchanged
successor proof and add the observed current cwnd to the replacement-start
diagnostic. Remove any now-unused production read seam.

Run the focused replacement, dynamic proof, stale identity, install CAS, and
path-reset tests.

## Task 4: Add one stale/monotonic regression slice

Add the next smallest test proving a stale identity cannot publish a handoff
and the previously owned higher floor cannot be lowered by a later current
sample. Implement only if Task 2 does not already satisfy it.

## Task 5: Local correctness and capacity gates

Run rustfmt, the root library/main/integration suites, release build,
established Clippy lane, runner/observer self-tests, docs, vendored Quinn and
quinn-proto gates, diff/provenance/secret checks, and the exact 32MiB release
capacity discriminator. Reject any unexpected regression; reject capacity at
or below `170 Mbit/s` without tuning.

## Task 6: Stage review and memory

Use the project code-review checklist for identity ownership, lock scope,
bounded proof work, fallback, predecessor drain, TCP/UDP/TUN/D16/Endpoint
regression, diagnostics, and missing tests. Repair any P0/P1 finding and rerun
affected gates. Record the formal evidence, implementation result, learning,
errors, HANDOFF, and TODO state.

## Task 7: Commit, push, and next acceptance

Create one coherent conventional commit and push the current branch. The next
Mac action is one fresh paired `m2-qualification`, not formal M2 and not a
repeat of `85d8772`. A clean qualification can only reopen formal M2; M3 stays
blocked until formal M2 and cleanup pass.
