# Knife14dx Relay Credit Results

Date: 2026-07-07

## Stage Goal

Replace the relay ready-read batch from a fixed local drain with a bounded
budget that cannot overshoot and that can respond to downstream pressure. The
immediate discriminator was whether the DW batch improvement was still hiding
terminal pending/close loss, or whether the remaining reverse-first loss had
moved to live egress pressure.

## Code Result

- Added deterministic relay batch tests for:
  - exact byte-budget enforcement without read overshoot;
  - downstream queue-pressure tapering;
  - data-before-EOF ordering after coalescing.
- Added relay diagnostics:
  `remote_batch_limit_bytes_min` and `remote_batch_limited`.
- Local gates passed:
  `cargo test relay_ --lib`,
  `cargo test tun_rx_drain --lib`,
  `cargo test downlink_backpressure --lib`,
  `cargo test tcp_downlink --lib`,
  `cargo test --lib`,
  and `git diff --check`.
- `.27` focused tests passed:
  `cargo test relay_remote_ready_burst --lib` and
  `cargo test relay_ready_batch_limit --lib`.

## VPS Run

- Bundle:
  `/tmp/mini_vpn/knife14dx_relay_credit_p1_30/mvpn_knife14dx_relay_credit_p1_30_usclient_suite_20260707_125810.tar.gz`
- Suite mode:
  `SERVER_EVIDENCE_CHECK=0 RUN_REVERSE_FIRST_P1=1 STOP_AFTER_REVERSE_FIRST_P1=1`.
- Direct baselines were healthy:
  `.27 -> .77` receiver about `274 Mbit/s`;
  `.27 <- .77` receiver about `280 Mbit/s`.
- Tunnel reverse-first P1 failed throughput:
  `25.7 Mbit/s` sender, `24.8 Mbit/s` receiver.

## Signals

- Close/lifecycle accounting was clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`,
  and `terminal_late_remote_payload=0`.
- QUIC loss/congestion/blocking remained clean:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`, and all parsed
  tx/rx blocked deltas were zero.
- The new relay cap worked: the data stream reached
  `remote_batch_bytes_max=524288` and did not exceed the configured cap.
- The new relay queue-pressure limiter did not engage:
  `remote_batch_limit_bytes_min=524288`, `remote_batch_limited=0`, and
  `global_rx_queue_used_max=106/1024`.
- The remaining live bottleneck was local egress/send-queue pressure:
  `tun_tx_dropped_delta=591`, `send_queue_max=892928`,
  `headroom_limited_calls=336`, and
  `headroom_deferred_bytes=28883630`.
- Stream cadence still had second-scale gaps:
  data stream `data_pending_gap_max_ms=3395`,
  `data_max_read_gap_ms=3814`, with repeated zero-throughput iperf intervals.

## Conclusion

Knife14dx proves that pending/close/reap no longer hides the loss point in this
clean reverse-first P1, and that the relay-to-main `mpsc` queue is not the
dominant pressure signal. The fixed batch cap is necessary but insufficient:
read-side credit must be coupled to the local socket/TUN egress state, not only
to the relay -> main-loop channel capacity.

## Next Patch Rule

Add an explicit per-flow relay read-credit channel from the main loop to the
relay read task. The credit should taper from the full ready-read batch toward
a small batch as the local socket `send_queue` approaches the flush/credit
edge, and should stop speculative ready-drain at or above the pause edge or
while TUN drop feedback is active. Do not continue by changing pool size,
sing-box, iperf3, stale slots, or static egress thresholds.
