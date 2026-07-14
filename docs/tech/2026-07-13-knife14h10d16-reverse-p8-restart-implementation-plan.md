# Knife14h10d16 Reverse P8 Restart Implementation Plan

Date: 2026-07-13
Status: **FROZEN VPS P8 FAILED; ACK-BARRIER REPAIR SELECTED**

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

- [x] Export/deploy the exact secret-free source and verify hashes.
- [x] Rehearse the unchanged H10d16 profile without iperf.
- [x] Run exactly one `REVERSE_FIRST_PARALLEL=8`, reverse-only, 60-second
  probe and stop before standard P1/full sweep.
- [x] Apply every architecture acceptance discriminator without tuning.

## Task 5 — Evidence And Decision

- [x] Retrieve and secret-scan the bounded evidence bundle.
- [x] Record the failed P8 result in a dedicated results document.
- [x] Update HANDOFF/TODO/AGENTS and learning memory with the repair result.
- [x] Commit the evidence stage, then either proceed to UDP/live-streaming or
  enter an architecture repair branch according to the stop rule.

Decision: the P8 timed out at `0.103 Mbit/s` after each data flow admitted
roughly one `128 KiB` quantum. TUN drops, pump saturation, smoltcp pressure,
QUIC loss, pool reconnect, and endpoint leakage were absent. The selected
root is lost per-flow ACK-completion evidence across a hard-pressure-dominated
zero snapshot. Continue with the ACK-barrier recovery spec and plan; do not
rerun or tune this baseline.
