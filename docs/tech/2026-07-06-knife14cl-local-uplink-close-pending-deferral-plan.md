# Knife14cl Local Uplink Close Pending Deferral Plan

Date: 2026-07-06

## Design Tree

- Branch A: ACK/window drain still under-budgeted.
  - Rejected by Knife14ck: `would_block=763`, `budget_exhausted=1`.
- Branch B: remote relay close defers pending correctly, but local uplink close
  bypasses that deferral and rearms with pending.
  - Supported by Knife14ck final close:
    `direction=local_to_remote reason=uplink_channel_closed pending=524906`.
- Branch C: dirty egress retention is missing entirely.
  - Partly rejected: dirty retention already considers `snapshot.send_queue >
    low_bytes`, but local close can rearm before that retention has a chance to
    drain pending.
- Branch D: egress-only close deferral is too short or starts too late.
  - Keep open after Branch B; do not tune grace until pending deferral is fixed
    and remeasured.

## Steps

1. Add focused tests for pending close deferral with a local
   `uplink_channel_closed` reason and empty-pending non-deferral.
2. Extract the duplicated pending-close deferral logic into one helper.
3. Use the helper from both `handle_relay_closed` and the local
   `EstablishedUplink::Closed` path.
4. Preserve the existing egress-only deferral path for empty pending.
5. Run focused tests, local regression gates, VPS reverse-first P1, parse the
   bundle, then record results and learning.

## Expected Signals

- New local close with pending produces a deferred-close-pending diagnostic
  instead of `tcp-handle-close ... pending>0 ... uplink_channel_closed`.
- `pending_relay_close` remains set until pending drains.
- If the VPS still fails, the next evidence should distinguish between
  post-deferral egress queue drain starvation and earlier data-path cadence.
