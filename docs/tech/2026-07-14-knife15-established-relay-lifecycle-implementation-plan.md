# Knife15 Established Relay Lifecycle Implementation Plan

Date: 2026-07-14

Status: **LOCAL IMPLEMENTATION COMPLETE; FRESH MACOS M0 PENDING**

## Task 1: Lock The Rejected Full-Open Timeout In RED

- Replace the old generic `relay_idle_timeout_shuts_down_stream` expectation
  with a deterministic test that crosses 90 seconds while fully open.
- Add the same assertion at the exact `run_relay_d16` coordinator seam.
- Prove both tasks still terminate through channel/lifecycle closure.

## Task 2: Introduce One Close-Timer Policy

- Add a writer `WritePending` signal before entering `write_all`/`flush`.
- Add one shared relay close-timer state machine.
- Arm 90 seconds only for pending write work and reset it on progress.
- Disarm after successful write completion while fully open.
- Arm/reset 10 seconds after local write-half completion.
- Use the policy in D16, native chunk, native permit, and generic engines.

## Task 3: Preserve Bounded Failure Tests

- Rename the pending-write timeout assertion to
  `stalled_write_timeout`.
- Preserve bidirectional progress reset behavior.
- Preserve local-FIN and D16 owned-payload drain tests.
- Run focused tests, then root tests and formatting.

## Task 4: Make macOS Evidence Finalization Immutable

- Add a testable finalized-bundle predicate.
- Refuse `snapshot`, `bundle`, or repeated `stop` mutations once a valid
  bundle/checksum pair exists.
- Build a bundle through temporary paths and publish it once.
- Add shell self-tests covering repeat-stop and overwrite rejection.

## Task 5: Add Target Readiness Before Rearm

- Before `start` mutates routes or launches mini_vpn, run a short direct
  iperf3 readiness transaction against the target.
- Fail closed on busy, broken-control, zero-throughput, or malformed output.
- Add result-parser self-tests and document failed-run rearm order.

## Task 6: Review, Record, And Accept

- Run shell syntax/self-tests, Rust focused/full tests, fmt, and diff checks.
- Review lifecycle, concurrency, bounded termination, D16, TCP/UDP/TUN
  regression, and operational evidence risks.
- Update HANDOFF, TODO, learning/error memory, architecture decision, and
  Knife15 macOS runbook.
- Commit coherent code/docs after all local gates pass.
- Run VPS checks if relevant, then provide the corrected macOS M0 command.

## Stop Rules

- An expected RED enters only its corresponding minimal implementation.
- A failure outside the specified relay/runner surfaces is analyzed before
  repair; no constant tuning or workload shortening is allowed.
- If the next M0 crosses 90 seconds but fails elsewhere, the new cause selects
  the next stage. Do not reopen endpoint pacing constants without its own
  discriminator.
