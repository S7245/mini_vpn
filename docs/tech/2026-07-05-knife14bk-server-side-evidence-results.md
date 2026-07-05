# Knife14bk Server-Side Evidence Results

Date: 2026-07-05

## Run

- Code under test: `804eec1`.
- Bundle:
  `/tmp/mini_vpn/knife14bk_server_evidence_20260705_134634/mvpn_knife14bk_server_evidence_usclient_suite_20260705_134634.tar.gz`
- Extracted local directory:
  `/tmp/mini_vpn/knife14bk_server_evidence_20260705_134634/`
- Probe:
  `RUN_REVERSE_FIRST_P1=1 STOP_AFTER_REVERSE_FIRST_P1=1 SERVER_EVIDENCE_CHECK=1`

## Baselines

- `.27 -> .77` direct forward: `321/274 Mbit/s` sender/receiver.
- `.27 <- .77` direct reverse: `326/297 Mbit/s` sender/receiver.
- `.33 -> .77` direct forward: `320/286 Mbit/s` sender/receiver.
- `.33 <- .77` direct reverse: `316/284 Mbit/s` sender/receiver.
- `.27`, `.33`, and `.77` clocks were NTP synchronized and within a few
  seconds of each other during preflight.
- `.33` sing-box was active, listening on UDP `8443`, and sampled logs did not
  show `fail auth`.

## Tunnel Result

- Reverse-first P1 over mini_vpn: `18.8/17.0 Mbit/s` sender/receiver.
- This is still the low-throughput class, but it is not the Knife14bj
  near-zero TUIC-stream-starvation shape.

## Server-Side Evidence

The new server evidence artifact was included:

`/tmp/mini_vpn/knife14bk_server_evidence_20260705_134634/mvpn_knife14bk_server_evidence_server_evidence_mtu1200_reverse_first_p1_20260705_134634.md`

Relevant signals:

- `.33` sing-box logged current-window TUIC inbound and `outbound/direct` opens
  to `.77:5201`; no current-window `fail auth` was observed.
- `.77` iperf3 journal for the probe showed the server-side reverse sender also
  finished at `67.1 MBytes / 18.8 Mbit/s` with bursty zero-throughput gaps.
- Therefore `.77` did not send a hidden high-throughput stream that mini_vpn
  lost locally. The low sender rate was visible at the target.

## mini_vpn Evidence

Low-RTT parser summary:

- `downlink_backpressure: pause_edges=10 resume_edges=10`
- `downlink_flush: no_send_capacity=7220 may_recv_false=7301
  send_queue_max=1048576 pending_total_max=16254`
- `pending_at_close: active_no_send_bytes=16254`
- `terminal_pending_reap: events=0 bytes=0`
- `tun_drops: tun_tx_dropped_delta=6070`
- `runtime_tun_egress: drop_events=2 drop_delta_total=5666 max_delta=4848`
- `tun_egress_feedback: pause_edges=2 drop_events=2 drop_delta_total=5666`
- `tuic_stream_pending: data_rx_bytes_max=70413616
  data_pending_gap_max_ms=4132`
- `tuic_stream_polling: data_poll_gap_max_ms=3403`
- `quic: max_lost_bytes_delta=0 max_congestion_events_delta=0
  max_tx_blocked_data_delta=0 max_rx_blocked_data_delta=0`

## Interpretation

Knife14bk moved the evidence from "TUIC stream is mostly idle and target may be
stalled before sending" to "target sender is also low because the local tunnel
path applies backpressure and eventually drops on TUN egress."

This run re-opens the local TUN egress/downlink backpressure branch with better
evidence:

- not auth/time/config/sing-box;
- not `.27/.33/.77` direct path capacity;
- not QUIC loss or QUIC blocking;
- not terminal pending reap;
- not relay-task polling starvation;
- active local limiter is the 1MiB smoltcp send queue, `may_recv_false`, TUN tx
  drops, and `active_no_send` pending at close.

## Tooling Note

The new `.33` sing-box evidence is useful but only tail-bounded, not strictly
time-bounded. It included current-window TUIC/outbound lines, but also older
VLESS noise. Before the next expensive run, the suite should make `.33`
evidence time- or marker-bounded so `fail auth` and current probe lines are not
buried in unrelated log history.

## Next Plan Candidate

Do not modify data-plane behavior before a small plan is confirmed. The next
patch should either:

- first improve the server-evidence log bounding to reduce attribution noise;
  or
- start a local TUN egress/backpressure fix that lowers sustained smoltcp send
  queue occupancy and TUN qdisc drops without returning to rejected blunt pacer
  tuning.
