# Knife14bw Stream Starvation Diagnostics Results

Date: 2026-07-06

Code under test:

- `4a12b18 fix(knife14bw): add reverse starvation diagnostics`

Bundle:

- `/tmp/mini_vpn/knife14bw_starvation_diag_20260706/mvpn_knife14bw_starvation_diag_usclient_suite_20260706_212704.tar.gz`

Local copy:

- `/tmp/mini_vpn/knife14bw_local/mvpn_knife14bw_starvation_diag_usclient_suite_20260706_212704.tar.gz`

## Goal

Knife14bw was a diagnostic-only stage after Knife14bv disproved the
close-egress-drain hypothesis. The goal was to distinguish a clean no-data
TUIC/server-to-client stream starvation window from local TCP
ACK/receive-window or tx-queue behavior.

## Preflight

- `.27` ran `4a12b18` with a clean worktree and release build.
- `.33` sing-box was active and UDP `:8443` was listening.
- `.77` iperf3 was active.
- Current-window TUIC handshake succeeded; no TUIC `fail auth` appeared.
- Direct baselines were healthy:
  - `.27 -> .77` reverse receiver: about `274 Mbit/s`.
  - `.33 -> .77` reverse receiver: about `281 Mbit/s`.

## Result

Reverse-first P1 still failed the Knife14 throughput target:

- iperf sender: `19.8 Mbit/s`
- iperf receiver: `18.9 Mbit/s`
- throughput shape: `low_average`
- interval pattern: burst/idle, not complete no-data. Several one-second
  intervals were `0`, with bursts around `43`, `61`, `62`, `109`, and
  `54.5 Mbit/s`.

Target-side iperf3 confirmed the same burst/idle sender pattern from `.77` to
`.33`, with cwnd mostly around `325-411 KBytes`.

## Key Signals

Signals that remain clean:

- QUIC client-side loss/congestion/blocking deltas stayed `0`.
- TUN RX/TX dropped deltas stayed `0`.
- TUN runtime egress drop events stayed `0`.
- `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`.
- `terminal_pending_reap=0` and `pending_at_close=0`.
- `local_write_pressure=0` and `global_rx_pressure=0`.

New reverse-window diagnostics:

- `tcp_reverse_window: events=10`
- `payload_bytes=248835`, `accepted_bytes=248835`
- `pending_max=0`
- `send_capacity_min=max=1048576`
- `send_queue_max=65536`
- `recv_queue_max=0`
- `may_recv_false=1`, only at the final `CloseWait`/close-tail sample.
- Live data samples were `Established active=true can_send=true may_recv=true`
  with `pending=0`.

Stream delivery was not Knife14bv-style tiny:

- `tuic_tcp_stream ... data_rx_bytes_max=72662685`
- `relay_remote_timing ... data_rx_bytes_max=72662685`
- data stream first byte was fast: `3ms`
- data stream max read/pending gap was about `5543ms`

Local tx-queue pressure was active:

- `downlink_backpressure: pause_edges=51 resume_edges=51`
- `max_pending_bytes=0`
- `max_tx_queue_bytes=588901`
- `downlink_flush ... send_queue_max=524280`
- `tun_flush_deferred=51`

Close-tail accounting:

- `terminal_late_remote_payload=1730017B/705`
- `egress_at_close=30976B`, classified `terminal_closed_no_send`
- `terminal_pending_reap=0`
- sing-box current-window close line was
  `connection download closed: stream 4 canceled by remote with error code 0`.

## Interpretation

Knife14bw disproves the narrow "TUIC stream delivered almost no bytes" branch
for this run. The data stream delivered about `72.7MB`, first byte was fast,
and the new reverse-window samples showed live local TCP could accept reverse
payloads with full send capacity and no app-owned pending backlog.

The active limiter in this run is local tx-queue/backpressure cadence:

- mini_vpn repeatedly accepted bytes into smoltcp, filling the local send queue
  near the high watermark;
- `downlink_backpressure` paused and resumed remote reads 51 times;
- the target sender showed matching burst/idle windows;
- QUIC/TUN/syscall/error surfaces stayed clean.

The `terminal_late_remote_payload` is real but occurs after the low-throughput
shape is already established. It is still useful close-tail accounting, not the
first hidden loss point for this run.

## Next Patch Proposal

Do not continue stale pool, iperf3, sing-box, TUN queue length, TUIC auth,
egress-pacer-only, or close-drain tuning from this evidence.

The next behavior patch should target local tx-queue pressure cadence, with
focused TDD first:

1. Add a deterministic test for burst/idle risk: if app-owned pending is `0`
   and smoltcp send queue briefly crosses the high watermark, remote reads
   should not oscillate every small drain/refill cycle.
2. Adjust downlink backpressure to use a steadier tx-queue pressure signal for
   reverse data, such as a lower resume cadence, minimum pause duration,
   or tx-queue-pressure budget that avoids immediate pause/resume thrash.
3. Preserve hard bounds: app-owned pending, tx queue, and remote channel
   pressure must remain bounded; no unbounded read-ahead.
4. Re-run one reverse-first P1 acceptance and require fewer pause/resume edges,
   smoother intervals, and throughput above the current 10-20 Mbit/s tier.

This is a behavior change and should be confirmed before coding.
