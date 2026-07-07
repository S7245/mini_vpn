# Knife14dy Read Credit Results

Date: 2026-07-07

## Stage Goal

Couple the relay read side to per-flow local egress state after Knife14dx proved
that the relay -> main-loop `mpsc` queue was not the pressure point. The stage
goal was to determine whether a main-loop -> relay read-credit channel could
prevent hidden close/pending/reap loss and reduce local socket/TUN overdrive.

## Code Result

- Added a per-flow `watch` read-credit channel from the main loop to the relay
  read task.
- Relay reads now pause when the per-flow egress guard is active and taper batch
  size as local send-queue pressure rises.
- Relay diagnostics now include read-credit update counters and minimum
  observed credit limit.
- Local gates passed:
  `cargo test relay_read_credit --lib`,
  `cargo test relay_remote_ready_burst --lib`,
  `cargo test relay_ --lib`,
  `cargo test tun_rx_drain --lib`,
  `cargo test downlink_backpressure --lib`,
  `cargo test tcp_downlink --lib`,
  `cargo test --lib`,
  and `git diff --check`.
- `.27` focused tests passed:
  `cargo test relay_read_credit --lib` and
  `cargo test relay_remote_ready_burst --lib`.

## VPS Run

- Bundle:
  `/tmp/mini_vpn/knife14dy_read_credit_p1_30/mvpn_knife14dy_read_credit_p1_30_usclient_suite_20260707_131037.tar.gz`
- Suite mode:
  `SERVER_EVIDENCE_CHECK=0 RUN_REVERSE_FIRST_P1=1 STOP_AFTER_REVERSE_FIRST_P1=1`.
- Direct baselines were healthy:
  `.27 -> .77` receiver about `288 Mbit/s`;
  `.27 <- .77` receiver about `280 Mbit/s`.
- Tunnel reverse-first P1 still failed:
  `22.8 Mbit/s` sender, `21.6 Mbit/s` receiver.

## Signals

- The read-credit mechanism was alive:
  data stream `read_credit_updates=86`,
  `read_credit_limit_bytes_min=65536`, and
  `remote_batch_limited=6`.
- The read-credit policy paused only at the end:
  `read_credit_pause_updates=1` after local egress had already reached the
  credit edge.
- Close/lifecycle accounting remained clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`, and
  no terminal late remote payload.
- QUIC loss/congestion/blocking remained clean.
- Local egress pressure still returned:
  parser `tun_tx_dropped_delta=2973`, runtime feedback
  `tx_dropped_delta=2163`, `send_queue_max=892928`,
  `headroom_limited_calls=3623`, and
  `headroom_deferred_bytes=63482206`.
- The relay stream still showed second-scale cadence gaps:
  data stream `data_max_read_gap_ms=3472` and
  `data_pending_gap_max_ms=3394`.

## Conclusion

Knife14dy proves that the per-flow read-credit channel is wired correctly, but
the first policy was one batch late. It reacted to actual `send_queue` and
`pending` after a remote payload had already entered the main loop. A 512KiB
batch could still be accepted and flushed with stale drain-credit before the
relay reader saw the new local pressure.

## Next Patch Rule

Move the credit/debt decision to projected pressure: `send_queue + pending +
incoming`. Publish projected read-credit before processing a remote payload,
and install projected pressure debt before `flush_downlink` can consume stale
drain-credit. This is a code-level headroom model change, not another static
threshold/pacer/pool/sing-box direction.
