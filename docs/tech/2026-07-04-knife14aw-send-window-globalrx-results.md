# Knife14aw results - send-window and global_rx diagnostics

Date: 2026-07-04

## Artifacts

- Code commit: `ef7364c`
- Local bundle:
  `/tmp/mini_vpn/mvpn_knife14aw_send_window_globalrx_usclient_suite_20260704_204329.tar.gz`
- Local extraction:
  `/tmp/mini_vpn/knife14aw_send_window_globalrx_204329/`
- Remote report:
  `/tmp/conn/mvpn_knife14aw_send_window_globalrx_usclient_suite_20260704_204329.md`
- Client log:
  `/tmp/conn/mvpn_accept_20260704_204329.log`

## Outcome

Knife14aw met the diagnostic goal but not the throughput goal.

The scoped VPS acceptance failed:

- reverse-first P1: `16.4/15.0 Mbit/s`;
- standard reverse P1 after a bad forward window: `38.2/37.1 Mbit/s`;
- full reverse: `18.3/17.2 Mbit/s`.

Post-suite direct baselines were healthy:

- `.27 <-> .77`: forward and reverse were both about `196 Mbit/s`;
- `.33 <-> .77`: forward and reverse were about `197 Mbit/s`.

## Clean Reverse-First Evidence

The clean reverse-first P1 window had:

- `global_rx_pressure: events=0 max_wait_ms=0.000 queue_used_max=258
  queue_capacity=1024`;
- `downlink_backpressure: pause_edges=28 resume_edges=28
  max_pending_bytes=588277`;
- `downlink_flush: attempts=7097 no_send_capacity=510
  send_window_samples=7097 send_capacity_min=1048576
  send_capacity_max=1048576 send_queue_max=1048576 recv_queue_max=0
  may_send_false=0 may_recv_false=0 no_send_streak_max=101
  no_send_pending_max=588277 send_slice_calls=6587
  accepted_bytes=55170027 zero=0 errors=0 budget_limited=3385
  tun_flush_failures=0 tun_flush_deferred=0`;
- `terminal_pending_reap: events=1 bytes=524627`;
- `pending_at_close: events=1 bytes=524627 terminal_events=1`;
- `tun_tx_dropped_delta=2283`;
- clean-window QUIC loss/congestion stayed zero:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_start_lost_bytes=0`, `max_start_congestion_events=0`,
  `min_cwnd=12000`.

The close-time socket snapshot was terminal:

```text
tcp-handle-close ... pending=524627 ... close_pending_class=terminal_closed_no_send
terminal_pending_reap_bytes=524627 tcp_state=Closed active=false can_send=false
may_send=false may_recv=false send_capacity=1048576 send_queue=2079 recv_queue=0
```

## Interpretation

Knife14aw rejects several remaining weak branches for the clean reverse-first
window:

- the public VPS path was not in the `10-20 Mbit/s` band;
- the relay `global_rx` channel had backlog but was not full;
- smoltcp configured tx capacity did not shrink;
- `send_slice` did not return zero or error;
- TUN flush did not fail or defer;
- clean-window QUIC did not lose packets or enter congestion.

The new evidence points to a missing local feedback edge. `send_slice` accepts
bytes into smoltcp, so `SocketCtx.downlink_pending` can fall to zero while the
smoltcp tx queue is still full (`send_queue_max=1048576`). The current
`global_rx_paused` decision only uses `downlink_pending`, so the relay can keep
feeding remote bytes after app-owned pending is empty even though the local TCP
tx queue is saturated and TUN egress is dropping.

Terminal pending at close is now accounted for, but it is more likely the tail
effect of the earlier tx-queue/TUN drain bottleneck than the first cause of low
reverse throughput.

## Next Direction

Knife14ax should make downlink backpressure aware of smoltcp tx queue pressure:

- treat per-socket pressure as the maximum of app-owned `downlink_pending` and
  smoltcp `send_queue`;
- pause `global_rx` when that pressure reaches the existing high watermark;
- resume only when both app-owned pending and socket tx queue pressure fall to
  the existing low watermark;
- log and parse the new tx-queue pressure fields so future reports cannot hide
  this case behind `pending_total=0`.

Do not change TUIC pool, sing-box, iperf3, TUN queue length, close/reap
predicates, or egress pacing defaults in this slice.
