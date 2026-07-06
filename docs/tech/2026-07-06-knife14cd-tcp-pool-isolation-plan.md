# Knife14cd TCP Pool Isolation Plan

Date: 2026-07-06

## Design Tree

1. Keep pool=1 and tune local egress first.
   - Rejected for this stage. Two pool=1 runs failed before local egress
     pressure became active, with tiny data-stream bytes and clean drops.
2. Raise default to pool=2.
   - Accepted. The A/B changed `conns=0` to `conns=0,1`, removed the slow
     first byte, and restored data delivery.
3. Raise default above pool=2.
   - Rejected. Current evidence proves the need to isolate concurrent
     control/data streams from the primary connection, not the need for larger
     pools.
4. Treat sing-box `connection download closed` as an auth/time/config issue.
   - Rejected for now. Current-window evidence did not show `fail auth`, and
     pool=2 changed the failure shape without changing server configuration.

## Tasks

1. Add/adjust tests for pool default/min/max parsing.
2. Make `MIN_TUIC_TCP_POOL=1` distinct from `DEFAULT_TUIC_TCP_POOL=2`.
3. Clamp runtime config with the min, not the default, so explicit pool=1
   remains valid.
4. Change the US-client suite default from pool=1 to pool=2.
5. Keep relay-writer flush semantic coverage but do not assign TUIC VPS
   causality to it.
6. Update Knife14cc results, Knife14cd spec/plan, and learning/error memory.
7. Run focused tests, script self-tests, full local gates, commit, push, and
   deploy to `.27`.
8. Run scoped VPS reverse-first P1 acceptance using suite defaults.
9. Parse the bundle and decide:
   - no-data returns: re-evaluate stream/pool architecture;
   - pool=2 stays live but low-average/drop-heavy: move to local egress/drop
     algorithm repair;
   - stable high and clean final drops/pending: advance Knife14 progress toward
     final regression.

## Risk Checks

- Pool=2 means an extra QUIC connection to the exit. Keep the max cap and
  explicit pool=1 escape hatch.
- The script default must match product default; otherwise VPS acceptance can
  silently test a different runtime than the code.
- Final acceptance must parse whole-suite tail summaries, not only the
  per-probe low-RTT attribution block.
- Do not summarize server evidence as `fail auth` unless the current-window log
  actually contains authentication failures.
