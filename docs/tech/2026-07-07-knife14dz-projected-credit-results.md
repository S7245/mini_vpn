# Knife14dz Projected Credit Results

Date: 2026-07-07

## Stage Goal

Move relay read credit from post-accept local pressure to projected local
pressure: `send_queue + pending + incoming`. The goal was to stop one stale
large remote batch from spending old drain credit and driving the local
smoltcp/TUN egress path to the hard edge.

## VPS Run

- Bundle:
  `/tmp/mini_vpn/mvpn_knife14dz_projected_credit_p1_30_usclient_suite_20260707_132432.tar.gz`
- Extracted local copy:
  `/tmp/mini_vpn/knife14dz_projected_credit_p1_30/`
- Direct baselines were healthy:
  `.27 -> .77` receiver about `274 Mbit/s`;
  `.27 <- .77` receiver about `280 Mbit/s`.
- Tunnel reverse-first P1 regressed badly:
  `0.0 Mbit/s` sender summary and `6.05 Mbit/s` receiver; the probe timed out.

## Signals

- The projected credit path did engage:
  `remote_batch_limited=29`, `read_credit_updates=240`, and
  `read_credit_pause_updates=2`.
- Pending/close/reap accounting stayed clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`, and
  no terminal late remote payload.
- TUN drops were reduced but not eliminated: `tun_tx_dropped_delta=842`.
- A new failure appeared in the QUIC receive path:
  `max_rx_blocked_stream_delta=1`, with the data stream showing
  `data_read_gap_max_ms=3474` and `data_pending_gap_max_ms=3279`.

## Conclusion

Projected pressure was directionally necessary, but coupling active local
pressure debt directly into the relay read hard pause was too strong. It
protected local egress by starving the QUIC stream receive window, which made
sender cadence worse than the previous post-accept policy.

## Next Rule

Separate "read QUIC into bounded local staging" from "flush local staging into
smoltcp/TUN". Pressure debt should constrain local flush/close accounting, but
must not hard-pause relay reads unless the receive window itself is full or
TUN drop feedback is active.
