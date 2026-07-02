# Knife14t: inactive pending downlink must use progress grace

## Grounding

- Project goal from `AGENTS.md`: mini_vpn is a VPN data-plane core, so a local
  socket lifecycle decision must not drop useful buffered bytes under pressure.
- Test bundle:
  `/tmp/mvpn_knife14s_usclient_suite_20260702_180337.tar.gz`.
- Knife14s improved the old forward uplink timer cap: the full sweep completed
  P1/P2/P4/P8 forward and reverse runs instead of the earlier P2 timeout shape.
- `remote_write_timeout` stayed absent and there were no send-slice errors or
  TUN flush failures on the remaining bad closes.
- The remaining signal is `dead_slot_reap` with non-zero `downlink_pending`,
  including `state=Relaying pending=1348494`, `state=Relaying pending=1307798`,
  `state=Relaying pending=1258063`, and several `state=Closing pending>0`
  cases.

## Problem

Knife14r only protected `pending_relay_close` slots. That covers
`state=Closing`, but not the earlier `state=Relaying` branch where smoltcp has
become inactive while a backlog still exists and the relay close signal has not
installed a deferred-close record yet.

The current predicate therefore still has this unsafe branch:

1. slot is not Listening;
2. `pending_relay_close` is absent;
3. smoltcp `is_active()` is false;
4. `downlink_pending` is non-empty;
5. `dead_slot_reap` immediately re-arms and clears the backlog.

## Goal

Use one progress-sensitive grace rule for all non-empty `downlink_pending`, not
only for deferred relay closes. A slot with pending downlink bytes should remain
alive while the backlog was recently observed or recently accepted by smoltcp,
then become reapable after no useful progress for the bounded grace window.

## Non-Goals

- Do not change TUIC/QUIC stream write behavior.
- Do not tune congestion control, MTU, or downlink high/low watermarks.
- Do not make inactive pending immortal; no-progress pending must still be
  bounded by `DEFERRED_CLOSE_PENDING_GRACE_SECS`.
- Do not claim the standalone P1 probe oddity is fixed by this stage.

## Invariants

- Any path that creates or flushes `downlink_pending` updates generic pending
  progress metadata.
- The first observation of non-empty pending starts a grace window.
- Any accepted downlink bytes refresh the grace window, even if a concurrent
  remote append makes the final pending length larger than the previous sample.
- Growth without accepted bytes does not fake drain progress.
- Active sockets with pending downlink are never reaped.
- Inactive sockets with pending downlink are reaped only after no progress for
  `DEFERRED_CLOSE_PENDING_GRACE_SECS`.
- Re-arming clears all pending progress metadata.

## Acceptance

- Unit tests cover inactive non-deferred pending grace, no-progress expiry, and
  pending progress bookkeeping.
- Local verification passes: `cargo test --lib client_tun`, full `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, and `git diff --check`.
- Next VPS run should show no `tcp-handle-close ... reason=dead_slot_reap ...
  pending>0` while `send_slice_errors=0` and `tun_flush_tx_failures=0`.
