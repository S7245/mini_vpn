# Knife14h10d16 ACK-Barrier Recovery Implementation Plan

Date: 2026-07-13
Status: **LOCAL PASS; FRESH FROZEN REVERSE P8 PENDING**

Architecture:
`docs/tech/2026-07-13-knife14h10d16-ack-barrier-recovery-architecture-spec.md`.

## Task 1 — Evidence And RED

- [x] Archive and classify the frozen reverse P8 architecture failure.
- [x] Add the hard-pressure zero-snapshot lost-evidence RED.
- [x] Add an eight-flow clean recovery RED.
- [x] Keep drop-debt, terminal, and no-barrier negative controls.

## Task 2 — Minimal State Repair

- [x] Retain the ACK barrier while hard pressure, drop debt, or terminal
  no-send dominates.
- [x] Consume it only on the first eligible clean zero snapshot.
- [x] Feed that transition as strict positive drain evidence without changing
  queue, quantum, timer, or capacity constants.
- [x] Preserve all existing Running/DrainOnly/Recovery behavior outside this
  interleaving.

## Task 3 — Local Gates And Review

- [x] Run focused phase, backlog-guard, D16 reader, and byte-ownership tests.
- [x] Run the exact D16/real-Quinn harness and full root regressions.
- [x] Run explicit `64/256/1024` concurrency and UDP sweep gates.
- [x] Run fmt, controlled clippy, shell self-tests, diff checks, and staged
  code review.
- [x] Update learning/error memory and commit one coherent repair.

## Task 4 — Fresh Frozen Reverse P8

- [ ] Export and deploy the exact secret-free source object with hashes.
- [ ] Rehearse the unchanged profile without iperf.
- [ ] Run one target-only reverse P8 for 60 seconds and stop before other
  probes.
- [ ] Apply throughput, interval, TUN, pump, D16 phase, QUIC, endpoint,
  lifecycle, and cleanup discriminators without tuning.

## Task 5 — Decision

- [ ] Record accepted or failed evidence in a dedicated results document.
- [ ] Update AGENTS/HANDOFF/TODO and learning memory.
- [ ] If PASS, continue Task 12 step 4 to UDP/live-streaming.
- [ ] If FAIL, select the newly observed architecture discriminator; do not
  retry constants or revive rejected branches.
