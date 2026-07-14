# Knife14h10d16 ACK-Barrier Recovery Implementation Plan

Date: 2026-07-13
Status: **READY FOR TDD**

Architecture:
`docs/tech/2026-07-13-knife14h10d16-ack-barrier-recovery-architecture-spec.md`.

## Task 1 — Evidence And RED

- [x] Archive and classify the frozen reverse P8 architecture failure.
- [ ] Add the hard-pressure zero-snapshot lost-evidence RED.
- [ ] Add an eight-flow clean recovery RED.
- [ ] Keep drop-debt, terminal, and no-barrier negative controls.

## Task 2 — Minimal State Repair

- [ ] Retain the ACK barrier while hard pressure, drop debt, or terminal
  no-send dominates.
- [ ] Consume it only on the first eligible clean zero snapshot.
- [ ] Feed that transition as strict positive drain evidence without changing
  queue, quantum, timer, or capacity constants.
- [ ] Preserve all existing Running/DrainOnly/Recovery behavior outside this
  interleaving.

## Task 3 — Local Gates And Review

- [ ] Run focused phase, backlog-guard, D16 reader, and byte-ownership tests.
- [ ] Run the exact D16/real-Quinn harness and full root regressions.
- [ ] Run explicit `64/256/1024` concurrency and UDP sweep gates.
- [ ] Run fmt, clippy, shell self-tests, diff checks, and staged code review.
- [ ] Update learning/error memory and commit one coherent repair.

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
