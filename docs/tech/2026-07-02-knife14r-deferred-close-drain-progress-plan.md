# Knife14r Plan: progress-sensitive deferred close reap

## Design Tree

1. Simply increase `DEFERRED_CLOSE_PENDING_GRACE_SECS`.
   - Rejected: hides the race but still kills slow, valid drains after the new
     deadline; also increases leak window for genuinely stuck slots.
2. Disable reap whenever `pending_relay_close` has pending downlink.
   - Rejected: violates bounded lifecycle; a dead local socket could hold a pool
     slot forever.
3. Refresh the defer deadline only when pending bytes decrease.
   - Chosen: preserves tail bytes while the local TCP side is still accepting
     data, and remains bounded when the drain stops.

## Tasks

1. Add/adjust unit tests so a deferred close is not reapable merely because the
   original close timestamp is old if pending has recently decreased.
2. Store deferred close drain progress in `SocketCtx`.
3. Update the drain path to refresh progress after `flush_downlink` reduces
   pending bytes.
4. Update `should_reap_slot` to use the last drain-progress timestamp for
   deferred pending.
5. Ensure rearm clears all deferred close progress fields.
6. Run local verification and stage code review.
7. Record stage learning and commit.

## Test Focus

- `reap_predicate_graces_deferred_close_pending_downlink`: old close timestamp,
  recent drain progress, still not reapable; no further progress past the grace
  becomes reapable.
- New progress bookkeeping test: pending decreases refresh the progress timestamp
  and stored pending baseline; equal/growing pending does not fake drain
  progress.
- Existing relay close defer/rearm test: deferred close initializes and clears
  the progress fields.
