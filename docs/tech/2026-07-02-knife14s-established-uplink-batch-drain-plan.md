# Knife14s Plan: bounded batch drain for established uplink

## Design Tree

1. Reduce the 5ms timer interval.
   - Rejected: treats a scheduling artifact as the data path; increases CPU
     cost and still leaves one-payload-per-tick logic intact.
2. Loop until smoltcp has no readable bytes.
   - Rejected: can let one hot flow monopolize the main loop under P8/P16.
3. Drain a bounded batch per dirty pass.
   - Chosen: removes the deterministic one-MSS-per-timer throttle while keeping
     fairness and channel backpressure.

## Tasks

1. Extract established-uplink handling into a testable helper.
2. Add TDD tests:
   - drains multiple payloads in one pass;
   - stops at `MAX_ESTABLISHED_UPLINK_BATCH`;
   - does not consume payloads when relay mpsc is full;
   - sends local `Finish` when data is drained and socket is `CloseWait`.
3. Replace the one-payload path in `process_listener_activity` with the helper.
4. Run local verification and code-review the hot path for fairness/backpressure.
5. Record stage learning and commit.

## Next VPS Acceptance

Run the same `knife14s` suite with `MINI_VPN_TUIC_TCP_POOL=1` first. The target
signal is forward throughput no longer being pinned near 1-2 Mbit/s with long
zero-bps gaps. Keep watching for:

- `remote_write_timeout` must remain absent;
- `tcp-handle-close ... dead_slot_reap ... pending>0` is still a separate
  follow-up if it remains;
- `.33/.77` service preflight must stay clean before trusting throughput.
