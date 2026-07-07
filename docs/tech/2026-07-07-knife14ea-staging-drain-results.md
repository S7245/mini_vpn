# Knife14ea Staging Drain Results

Date: 2026-07-07

## Stage Goal

Fix the Knife14dz regression by allowing the relay reader to keep draining the
TUIC/QUIC stream into bounded local staging while local egress pressure debt
constrains only the smoltcp/TUN flush path.

## VPS Run

- Bundle:
  `/tmp/mini_vpn/mvpn_knife14ea_staging_drain_p1_30_usclient_suite_20260707_133922.tar.gz`
- Extracted local copy:
  `/tmp/mini_vpn/knife14ea_staging_drain_p1_30/`
- Direct baselines were healthy:
  `.27 -> .77` receiver about `280 Mbit/s`;
  `.27 <- .77` receiver about `264 Mbit/s`.
- Tunnel reverse-first P1 improved from Knife14dz but remained low:
  `27.5/26.3 Mbit/s`.

## Signals

- The QUIC receive-window regression disappeared:
  all parsed rx/tx blocked, loss, and congestion deltas were zero.
- Local drop pressure was clean:
  parser and runtime `tun_tx_dropped_delta=0`.
- Read credit no longer paused on the data stream:
  `read_credit_pause_updates=0`.
- Close/reap pending stayed clean:
  `terminal_pending_reap=0` and `pending_at_close=0`.
- Remaining failure was cadence:
  `data_read_gap_max_ms=4523`, `data_pending_gap_max_ms=4522`, repeated zero
  iperf intervals, one terminal late remote payload event, and
  `egress_at_close=164221`.
- Relay read-gap ACK hints existed (`relay_gap_hint_attempts=110`), but each
  used only the active-flow budget.

## Conclusion

The architecture split was correct: keep QUIC stream reads moving and bound the
local flush path separately. That removed QUIC receive blocking and TUN drops,
but did not restore stable high throughput. The remaining signal was
ACK/window cadence during relay read gaps, not hidden close/reap loss.

## Next Rule

Treat relay read gaps as a stronger ACK/window starvation signal than ordinary
active-flow downlink work. A relay gap hint should be allowed to use the
pressure-sized ACK drain budget while still respecting TUN feedback and local
egress headroom.
