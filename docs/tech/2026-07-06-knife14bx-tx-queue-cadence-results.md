# Knife14bx Tx Queue Cadence Results

Date: 2026-07-06

## Commits And Artifacts

- Code commit: `b4fc5b4` (`fix(knife14bx): add tx queue backpressure headroom`)
- VPS bundle:
  `/tmp/mini_vpn/knife14bx_tx_queue_20260706/mvpn_knife14bx_tx_queue_usclient_suite_20260706_214754.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14bx_local/extract/`

## Run Shape

- Client: `.27` (`43.172.75.27`), repo at `b4fc5b45`, clean worktree.
- Exit: `.33` (`43.153.32.33`), sing-box active, UDP `:8443` listening.
- Target: `.77` (`43.130.32.77`), iperf3 active, TCP `:5201` listening.
- Suite mode: `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
  `SERVER_EVIDENCE_CHECK=1`, `EXIT_TO_TARGET_IPERF_CHECK=1`, `MTU=1200`,
  `MINI_VPN_TCP_DIAG=1`.
- Startup config: `high=524288`, `low=131072`,
  `tx_queue_pause_high=917504`, `tx_queue_resume_high=524288`.

## Baselines

- `.27 -> .77` direct forward: `319/283 Mbit/s` sender/receiver.
- `.27 <- .77` direct reverse: `304/280 Mbit/s` sender/receiver.
- `.33 -> .77` direct forward: `305/253 Mbit/s` sender/receiver.
- `.33 <- .77` direct reverse: `313/288 Mbit/s` sender/receiver.
- No current-window `.33` TUIC `fail auth` was present. The current-window
  sing-box lines showed TUIC inbound and direct outbound to `.77:5201`.

## Result

Reverse-first P1 improved but still failed the Knife14 throughput goal:

- iperf sender: `27.4 Mbit/s`
- iperf receiver: `26.4 Mbit/s`
- shape: `low_average`
- interval profile: `overall_avg_mbps=26.355`, `tail_avg_mbps=19.400`,
  `tail_min_mbps=0.000`

This is better than Knife14bw's `19.8/18.9 Mbit/s`, but it is still the same
burst/idle low-average class rather than a stable high-throughput pass.

## Key Signals

- `downlink_backpressure`: `pause_edges=28`, `resume_edges=28`,
  `max_pending_bytes=0`, `max_tx_queue_bytes=980698`,
  `tx_queue_pause_high=917504`.
- `downlink_flush`: `tun_flush_deferred=446`, `send_queue_max=917482`,
  `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`.
- `tcp_reverse_window`: `pending_max=0`, `send_capacity_min=1048576`,
  `send_queue_max=65536`, `may_recv_false=0`, `active_false=0`,
  `can_send_false=0`.
- `tuic_tcp_stream`: data stream `rx_bytes_max=100972827`,
  `reads_max=8007`, `data_read_gap_max_ms=3900`.
- `tuic_stream_pending`: data stream `data_pending_gap_max_ms=3900`.
- `tun_drops`: `tun_tx_dropped_delta=0`, `tun_rx_dropped_delta=0`.
- `quic`: no loss/congestion/blocking deltas.
- `terminal_pending_reap`: `0` events, `0` bytes.
- `terminal_late_remote_payload`: `2000200` bytes across `864` events after
  the socket was already terminal closed.
- `egress_at_close`: `12169` bytes, classified `terminal_closed_no_send`;
  no send-capable close-drain candidate.

## Interpretation

Knife14bx validated the tx_queue headroom idea as a partial improvement:
pause/resume churn dropped from Knife14bw's `51/51` to `28/28`, and P1 receiver
throughput rose from `18.9` to `26.4 Mbit/s`.

It did not close the branch. The evidence now points at a new mismatch inside
the local downlink cadence:

- global_rx backpressure now waits until tx_queue reaches the hard cap
  (`917504` bytes);
- but `DownlinkEgressPacer::allow_remote_payload_flush` still treats the soft
  high watermark (`524288` bytes) as the flush deferral point;
- the run accumulated `tun_flush_deferred=446` with no app pending, no TUN
  drops, no send-slice errors, and no QUIC pressure.

So the remaining bottleneck is not stale pool, iperf3, sing-box, QUIC loss,
TUN drops, or close/reap loss. It is still mini_vpn local downlink cadence, now
narrowed to the disagreement between remote-read tx_queue headroom and
immediate flush deferral around the old soft high.

## Next Candidate Plan

Do not immediately change behavior without confirmation. The next candidate
patch should be small and TDD-first:

1. Keep app-owned pending data hard-bounded at the existing high/low values.
2. Keep true TUN drop feedback unchanged.
3. Align tx_queue-only immediate flush deferral with the tx_queue hard cap, not
   the soft high, while preserving the existing forced flush for non-empty app
   pending.
4. Add deterministic tests showing `allow_remote_payload_flush` does not defer
   tx_queue-only payloads at soft high but still defers at the derived hard cap.
5. Re-run the same reverse-first P1 VPS suite and compare throughput,
   `pause_edges`, `tun_flush_deferred`, TUN drops, send-slice errors, and
   terminal accounting.

Current overall Knife14 progress estimate: about `84%`. It rose slightly
because the patch moved the metric in the right direction and narrowed the
active root, but it cannot move to the high-confidence range until a scoped VPS
run leaves the low-average tier.
