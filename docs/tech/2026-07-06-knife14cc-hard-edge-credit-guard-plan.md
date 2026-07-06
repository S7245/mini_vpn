# Knife14cc Hard-Edge Credit Guard Plan

Date: 2026-07-06

## Design Tree

1. Static threshold tuning is rejected.
   - Knife14ca/bz proved moving fixed thresholds trades throughput against
     egress drops without closing the root.
2. Removing progress credit is rejected.
   - Knife14cb showed progress-clocked accept is what moved reverse throughput
     from the `10-20 Mbit/s` band to `152 Mbit/s`.
3. Hard-edge credit guard is the next bounded algorithm.
   - Keep credit based on observed local egress drain.
   - Reserve a small fraction of the clean-to-hard credit span below the hard
     pause threshold so accepted bytes do not land directly on the drop edge.

## Tasks

1. Add a focused failing test showing current credit plans to hard pause.
2. Add a derived hard-edge guard helper.
3. Cap credit-spend availability at `pause - guard`.
4. Add diagnostics for guard-deferred bytes.
5. Update parser self-tests if aggregate logs expose the new fields.
6. Run local gates, commit/push, then re-run scoped VPS acceptance.

## Risk Checks

- The guard must never reduce clean-headroom accepts below the clean threshold.
- The guard must be derived from existing watermarks, not another env tune.
- Credit consumed must still be based on actual accepted bytes.
- Pending bytes must remain pending and visible when the guard defers them.
