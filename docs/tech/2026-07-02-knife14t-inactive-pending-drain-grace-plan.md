# Knife14t Plan: progress grace for all pending downlink

## Design Tree

1. Increase `DEFERRED_CLOSE_PENDING_GRACE_SECS`.
   - Rejected: it only makes the data-loss window wider and still does not cover
     the `state=Relaying` non-deferred branch correctly.
2. Skip dead-slot reap whenever `downlink_pending` is non-empty.
   - Rejected: a genuinely closed local socket could leak a listener slot and
     fake-IP reference indefinitely.
3. Track generic pending progress and use it in the reap predicate.
   - Chosen: preserves useful tail bytes while smoltcp is still accepting them,
     and remains bounded once progress stops.

## Tasks

1. Add failing unit coverage for inactive non-deferred pending with recent
   progress and with expired no-progress age.
2. Add generic pending progress metadata to `SocketCtx`.
3. Update downlink enqueue/flush paths to record first observation, accepted
   bytes, and no-progress growth correctly.
4. Make `should_reap_slot` consult generic pending progress before the inactive
   hard-reap branch.
5. Clear all new metadata during rearm and initialize it when relay close
   defers on pending bytes.
6. Run local verification and stage code review.
7. Record `.learnings/LEARNINGS.md` / `.learnings/ERRORS.md` and commit.

## Next VPS Acceptance

Run the next suite as `knife14t`. The key acceptance signal is lifecycle, not
throughput: no pending-bearing `dead_slot_reap` lines when send-slice and TUN
flush error counters remain zero. Keep the existing `.33` / `.77` preflight and
binary commit/checksum evidence before trusting the run.
