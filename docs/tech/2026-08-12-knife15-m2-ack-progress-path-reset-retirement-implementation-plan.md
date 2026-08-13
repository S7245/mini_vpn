# Knife15 M2 ACK-Progress Path Reset Retirement Implementation Plan

Date: 2026-08-12

Status: **TASKS 1-7 COMPLETE; PAIRED QUALIFICATION NEXT; FORMAL M2 AND M3
BLOCKED**

Architecture:
`docs/tech/2026-08-12-knife15-m2-ack-progress-path-reset-retirement-architecture-spec.md`.

## Task 1: Lock the formal failure with one RED

Replace the old positive path-reset policy expectation with an exact
regression: a writer remains `Pending` for the existing bound, ACKed bytes
continue to advance, and the same stable connection's black-hole counter
advances. Require `EndpointRecoveryAction::None`.

Run only this test and confirm it fails because the current policy returns
`ResetConnectionPath`, not because of harness or timing setup.

## Task 2: Remove the rejected policy authority

Remove path-degradation eligibility, begin/reset state, action and trigger
variants from `EndpointRecoveryState`. Preserve the earlier exact ACK-stall
rebind branch and UDP recovery logic. Run Task 1 GREEN plus the focused
Endpoint recovery policy tests.

## Task 3: Remove the execution and adapter surface

Remove the monitor's `ResetConnectionPath` executor, slot-owned
`reset_path_if_owned()` method, path-reset outcome/snapshot/preparation types,
and their mini_vpn tests. Keep current-service handoff floor publication in
replacement preparation. Confirm mini_vpn has no production
`Connection::path_changed()` call.

## Task 4: Lock preserved recovery behavior

Run or add the smallest focused regressions proving:

1. exact ACK stall still selects Endpoint rebind;
2. ACK progress plus repeated black-hole movement remains non-destructive;
3. generation replacement handoff stays monotonic and identity owned;
4. successor proof and predecessor drain remain unchanged.

## Task 5: Local correctness and capacity gates

Run rustfmt, root library/main/integration suites, release build, established
Clippy lane, shell and runner/observer self-tests, docs, vendored Quinn and
quinn-proto suites, diff/provenance/secret checks, and the exact 32MiB release
capacity discriminator. Reject an unexpected regression; reject capacity at
or below `170 Mbit/s` without tuning.

## Task 6: Stage review and memory

Use the project code-review checklist for recovery authority, current-flow
ownership, ACK semantics, replacement/drain behavior, TCP/UDP/TUN/D16/
Endpoint regressions, diagnostics, and missing tests. Repair P0/P1 findings
and rerun affected gates. Record results in HANDOFF, TODO, learnings, and
errors.

## Task 7: Commit, push, and next acceptance

Completed in reviewed commit `0a3cc9c`. Because Rust production recovery
behavior changed, the next Mac action is one fresh paired
`m2-qualification`, not formal M2. A clean qualification reopens one fresh
formal M2; M3 remains blocked until formal M2 and cleanup pass.
