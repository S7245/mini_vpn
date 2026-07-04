# Knife14av results - post-iperf close-tail reporting

Date: 2026-07-04

## Artifacts

- Code commit: `b7e10b1`
- Local bundle:
  `/tmp/mini_vpn/mvpn_knife14av_post_iperf_close_tail_usclient_suite_20260704_202011.tar.gz`
- Local extraction:
  `/tmp/mini_vpn/knife14av_post_iperf_close_tail_202011/`
- Remote report:
  `/tmp/conn/mvpn_knife14av_post_iperf_close_tail_usclient_suite_20260704_202011.md`
- Client log:
  `/tmp/conn/mvpn_accept_20260704_202011.log`

## Outcome

Knife14av fixed the report timing gap but did not meet the throughput goal.

The intended script behavior was active:

- probe reports included `post_iperf_metrics_settle_secs=2`;
- `tcp-handle-close pending>0` lines that arrived just after `iperf3` were
  included in the matching per-probe attribution summary;
- the raw log and per-probe summaries no longer disagreed about terminal
  pending tails.

The scoped VPS acceptance still failed:

- reverse-first P1: `11.4/10.1 Mbit/s`;
- standard reverse P1: `18.1/17.0 Mbit/s`;
- full reverse: `24.1/23.3 Mbit/s`.

## Clean Reverse-First Evidence

The reverse-first P1 window had:

- `global_rx_pressure: events=0`;
- `local_write_pressure: events=0`;
- `downlink_backpressure: pause_edges=10 resume_edges=10
  max_pending_bytes=568596`;
- `downlink_flush: attempts=4643 no_send_capacity=136
  accepted_bytes=35467639 send_slice_zero=0 send_slice_errors=0
  tun_flush_failures=0 tun_flush_deferred=0`;
- `terminal_pending_reap: events=1 bytes=69769`;
- `pending_at_close: events=1 bytes=69769 terminal_events=1`;
- `tun_tx_dropped_delta=1170`;
- QUIC clean-window loss/congestion stayed zero:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_start_lost_bytes=0`, `max_start_congestion_events=0`.

The corresponding raw close line was captured before the summary:

```text
tcp-handle-close ... pending=69769 ... close_pending_class=terminal_closed_no_send
terminal_pending_reap_bytes=69769 tcp_state=Closed active=false can_send=false
```

## Interpretation

Knife14av proved the parser no longer hides post-iperf close tails. The failure
is therefore real data-plane behavior, not a reporting-order artifact.

The current evidence rejects these branches for this run:

- iperf3 service failure;
- sing-box service failure;
- stale TUIC TCP pool slots;
- clean-window QUIC loss or congestion;
- `send_slice` zero/error as the primary loss point;
- TUN flush syscall failure or deferred immediate flush.

The remaining blind spot is local receive-window/downlink drain visibility:

- `global_rx_pressure=0` only proves the relay task did not wait 5ms or more on
  `back_tx.send`; it does not prove the channel had no backlog;
- `no_send_capacity` counts attempts but does not say whether smoltcp was out of
  tx-buffer capacity, had `may_send=false`, or carried queued bytes at close;
- close logs show `can_send=false`, but not `send_capacity`, `send_queue`,
  `recv_queue`, `may_send`, or `may_recv`.

## Next Direction

Knife14aw should add behavior-neutral diagnostics for:

- smoltcp send-window snapshots during downlink flush attempts;
- close-time socket queue/window snapshots;
- relay-to-main-loop `global_rx` channel occupancy.

Do not change close/reap, egress pacing, TUN queue length, TUIC pool, iperf3, or
sing-box behavior in that diagnostic slice.
