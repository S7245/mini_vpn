# Knife14bv Close-Egress Drain Results

Date: 2026-07-06

Code under test:

- `3e34133 fix(knife14bv): expose close egress pressure`
- `8f68a89 fix(knife14bv): defer close while egress queue drains`

Bundle:

- `/tmp/mini_vpn/knife14bv_close_egress_20260706/mvpn_knife14bv_close_egress_usclient_suite_20260706_194841.tar.gz`

## Goal

Knife14bv tested whether the remaining reverse-first throughput failure was
caused by rearming a local TCP slot while smoltcp still had send-capable egress
queued at close.

Acceptance wanted reverse-first P1 to leave the 10-20 Mbit/s tier and wanted
the new close accounting to distinguish app pending, smoltcp egress pending,
terminal pending reap, and late remote payload.

## Preflight

- `.27` client ran `8f68a89` with a clean worktree and release build.
- `.33` sing-box was active, UDP `:8443` was listening, and TUIC handshake
  succeeded.
- `.77` iperf3 was active.
- Client/exit/target clocks were synchronized within about one second.
- Direct baselines were healthy:
  - `.27 -> .77` reverse receiver: about `282 Mbit/s`.
  - `.33 -> .77` reverse receiver: about `275 Mbit/s`.
- No current-window `.33` TUIC auth failure was observed. The noisy
  `REALITY: processed invalid connection` lines were stale internet background
  traffic on the unrelated REALITY inbound, not the TUIC acceptance flow.

## Result

Reverse-first P1 failed much harder than Knife14bu:

- iperf sender: `0.315 Mbit/s`
- iperf receiver: `0.113 Mbit/s`
- shape: `no_data`
- intervals: almost all zero, with only a small burst around second 10 and
  another around second 22.

Target-side iperf evidence showed the sender from `.77` to `.33` stalled too:

- `0.00-1.00 sec`: `640 KBytes`, `5.24 Mbit/s`, cwnd `109 KBytes`
- `1.00-22.00 sec`: mostly `0.00 bits/sec`, cwnd stayed `109 KBytes`
- `22.00-23.00 sec`: `512 KBytes`, `4.19 Mbit/s`
- total sender: `1.12 MBytes`, `315 Kbit/s`

## Key Signals

Clean local surfaces:

- `downlink_backpressure: pause_edges=0 resume_edges=0`
- `tun_tx_dropped_delta=0`
- `tun_rx_dropped_delta=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `tun_flush_deferred=0`
- `terminal_pending_reap=0`
- QUIC client-side loss/congestion/blocking deltas stayed `0`.

Close and terminal signals:

- `pending_at_close=0`
- `egress_at_close=1`, `14824B`, classified
  `terminal_closed_no_send`, not drain-candidate.
- No `tcp-deferred-close-egress` line appeared; the new close-egress drain
  behavior was not the active path.
- One terminal closed socket recorded `terminal_late_remote_payload=241304B/9`
  after local TCP had already closed.

Downlink starvation signals:

- The data stream received very little before close:
  `tuic-tcp-stream-close stream=4 rx_bytes=723424 reads=27`.
- TUIC stream read gaps were large:
  - `pending_gap_ms=20566`
  - `max_read_gap_ms=20567`
  - `max_poll_gap_ms=5000`
- Relay diagnostics matched the same shape:
  `remote_to_global_rx_bytes=723424`, `remote_reads=27`,
  `max_remote_read_gap_ms=20567`.

Server-side sing-box evidence in the current window:

- TUIC inbound from `.27` opened two `5201` flows at `19:49:35`.
- One data flow closed at `19:50:12` with
  `connection download closed: stream 4 canceled by remote with error code 0`.
- No TUIC `fail auth` appeared in the current acceptance window.

## Interpretation

Knife14bv-b disproves the narrow close-egress-drain hypothesis as the
throughput root for this run.

The failure happened before close cleanup became relevant: the target sender was
already stalled for most of the 30 second window, while mini_vpn local
backpressure, TUN drops, send-slice errors, and QUIC loss/congestion counters
were all clean. The only strong data-plane signal is long TUIC TCP stream
pending/read gaps and very low remote-to-global-rx delivery.

That points to a different branch:

- server-to-client TUIC stream delivery is starving or blocked before mini_vpn
  can feed smoltcp, or
- the local TCP endpoint/ACK behavior is closing or shrinking the receive path
  in a way that makes sing-box stop reading from `.77`, visible as `.77` cwnd
  stuck around `109 KBytes`.

The next patch should not extend close-drain behavior. It should first add
bounded diagnostics that separate these two possibilities:

1. TUIC stream delivered no bytes because the server side stopped sending or was
   blocked.
2. TUIC stream had bytes available but mini_vpn relay scheduling failed to drain
   them.
3. mini_vpn delivered bytes into smoltcp, but local TCP ACK/receive-window state
   caused the upstream sender to stall before the close boundary.

## Proposed Next Step

Before a behavior patch, add a focused Knife14bw observability/TDD slice:

- Add relay-side progress accounting for read-pending duration versus actual
  remote read bytes, preserving existing `tuic-tcp-stream-*` labels.
- Add per-flow local TCP receive/window lifecycle logging around the data handle
  while reverse traffic is live, not only at close.
- Add parser labels for `tuic_stream_starved`, `target_sender_stalled`, and
  `terminal_closed_late_payload`.
- Run one short repeat acceptance after the diagnostic-only patch. If the same
  no-data shape repeats with no local pressure, then choose between TUIC receive
  window/stream polling changes and local TCP ACK/window behavior with a
  bounded behavior patch.
