# Knife14o spec - grace deferred close pending downlink

Date: 2026-07-02

## Grounding

The `71da5aa` live run at
`/private/tmp/mvpn_knife14n_usclient_suite_20260702_133304.tar.gz` completed and
made real progress:

- VPS preflight passed for `.33` and `.77`.
- The P1 post-run quiet guard passed and full sweep executed.
- Full forward P1/P2/P4 reached `192`, `187`, and `143 Mbits/sec` receiver.
- Full reverse P1/P2/P4/P8 all exited `0`.

The remaining byte-loss pattern is now specific:

- `handle_relay_closed` sees `remote_eof` while `downlink_pending` is non-empty
  and moves the socket context into deferred close.
- The next dead-slot sweep can still reap the same slot while it has pending
  downlink, for example `state=Closing pending=4950956`.
- That rearm clears the pending tail and is followed by `local_channel_closed`
  relay events.

This means the deferred close path exists, but the low-frequency reap path can
preempt it before dirty-set flushing has a chance to drain the accepted downlink
tail.

## Problem

Knife14g intentionally allowed truly inactive sockets with pending bytes to be
reaped, because pending bytes might no longer be deliverable. Knife14n shows a
narrower case that should be treated differently:

- the relay close has already been observed;
- `pending_relay_close` is installed;
- `downlink_pending` contains bytes accepted from the remote relay;
- the dirty set is supposed to keep flushing those bytes before the final rearm.

If `reap_dead_slots` immediately aborts this slot, the deferred close invariant is
broken and the pending tail is dropped.

## Design

Add a short grace window for deferred relay closes with pending downlink:

- when `handle_relay_closed` defers close, record `pending_relay_close_since_secs`;
- `should_reap_slot` preserves `pending_relay_close + downlink_pending` until the
  grace window expires, even if smoltcp reports the socket inactive;
- after the grace window, the hard reap remains allowed so a permanently
  undeliverable tail cannot leak forever;
- inactive pending without a deferred relay close still reaps as before.

## Non-Goals

- No TUIC TCP pool change.
- No congestion-control change.
- No TUN buffer-size change.
- No attempt to preserve arbitrary inactive pending forever.

## Acceptance

- A predicate test proves deferred close pending is not reaped before grace.
- The same test proves it reaps after grace.
- Existing inactive-pending hard reap behavior remains for non-deferred slots.
- Existing relay close/deferred close tests still pass.
- Full library tests and clippy pass.
