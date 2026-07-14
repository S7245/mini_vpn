# Knife14h10d16 Reverse P8 Restart Implementation Plan

Date: 2026-07-13
Status: **LOCAL PASS; FROZEN VPS P8 PENDING**

Architecture:
`docs/tech/2026-07-13-knife14h10d16-reverse-p8-restart-architecture-spec.md`.

## Task 1 — Runner RED

- [x] Add a self-test requiring a reverse-first probe helper.
- [x] Require `REVERSE_FIRST_PARALLEL=8` to reach the probe as reverse-only
  parallel `8` with an unambiguous `p8` artifact label.
- [x] Require positive-integer validation and help/default attribution.

## Task 2 — Minimal Backward-Compatible Seam

- [x] Add `REVERSE_FIRST_PARALLEL=1` without changing existing behavior.
- [x] Validate it before output-directory, route, TUN, process, or iperf
  mutation.
- [x] Route the existing reverse-first call through the tested helper.
- [x] Print it in the report and usage text.

## Task 3 — Local Review Gate

- [x] Run suite shell syntax and self-test.
- [x] Run low-RTT probe syntax and self-test.
- [x] Review default compatibility, quoting, labels, early validation,
  lifecycle cleanup, secrets, and missing-test risk.
- [x] Run `git diff --check` and commit the orchestration stage.

## Task 4 — Frozen VPS P8

- [ ] Export/deploy the exact secret-free source and verify hashes.
- [ ] Rehearse the unchanged H10d16 profile without iperf.
- [ ] Run exactly one `REVERSE_FIRST_PARALLEL=8`, reverse-only, 60-second
  probe and stop before standard P1/full sweep.
- [ ] Apply every architecture acceptance discriminator without tuning.

## Task 5 — Evidence And Decision

- [ ] Retrieve and secret-scan the bounded evidence bundle.
- [ ] Record the accepted or failed P8 result in a dedicated results document.
- [ ] Update HANDOFF/TODO/AGENTS and learning memory.
- [ ] Commit the evidence stage, then either proceed to UDP/live-streaming or
  enter an architecture repair branch according to the stop rule.
