# Knife14bw Stream Starvation Diagnostics Plan

Date: 2026-07-06

## Grounding

Knife14bv disproved the close-egress drain hypothesis for the current
reverse-first P1 failure. The failed window had healthy direct baselines, no
current-window TUIC auth failure, no clean-window QUIC loss/congestion/blocking,
no local downlink backpressure, no TUN drops, no send-slice zero/errors, and no
deferred close-egress line. The target sender also stalled, while mini_vpn only
received about `723424` bytes on the TUIC data stream with a roughly `20567ms`
read/pending gap.

The next useful step is not a behavior patch. Knife14bw should make the next
VPS run able to distinguish server-to-client TUIC stream starvation from local
TCP ACK/window behavior without changing pacing, close-drain, stale pools, or
service configuration.

## Stage Goal

Add bounded diagnostics that answer, for the next clean reverse-first P1 run:

- Did TUIC deliver reverse bytes steadily, or did the data stream stall with a
  long pending/read gap and tiny received bytes?
- When mini_vpn accepted remote payloads, what did the local smoltcp TCP window
  look like: active, can_send, may_recv, send capacity, send queue, recv queue?
- Did terminal late payload occur after a closed local socket, and is it
  explicitly labeled rather than hidden inside generic close accounting?
- Did the target-side iperf sender stall while local mini_vpn pressure and QUIC
  client-side loss/congestion were clean?

## Non-goals

- Do not change TCP close-drain, deferred close, stale pool, egress pacing,
  TUN queue length, iperf3, sing-box, or QUIC congestion behavior in this
  stage.
- Do not store TUIC credentials, passwords, private keys, or host secrets in
  docs, scripts, logs, or learning memory.
- Do not interpret one VPS run as a permanent root cause unless the new labels
  and raw evidence agree.

## Implementation Plan

1. Add a `tcp-reverse-window` diagnostic line in `handle_remote_payload` after
   remote payload bytes are accepted/flushed into smoltcp.
2. Rate-limit that line to the first payload and every five event-loop seconds,
   with immediate refresh when the socket becomes inactive, cannot send, or
   cannot receive.
3. Reset the diagnostic rate-limit state on `rearm_socket` so listener slot
   reuse cannot inherit stale sampling state.
4. Extend the low-RTT parser with a `tcp_reverse_window` summary and labels:
   `tuic_stream_starved`, `target_sender_stalled`, and
   `terminal_closed_late_payload`.
5. Keep the suite report grep whitelist aligned so VPS bundles surface the new
   metric lines and summary fields.

## Acceptance

Local acceptance:

- Focused Rust tests cover reverse-window line formatting, rate limiting, and
  rearm cleanup.
- `scripts/knife14b-lowrtt-probe.sh --self-test` includes a Knife14bv-shaped
  no-data sample and reports the new labels.
- Shell syntax, Rust library tests, and release build pass.

VPS acceptance:

- Run one scoped reverse-first P1 window from `.27` against `.33` and `.77` with
  the usual service preflight and server evidence.
- If throughput remains in the no-data/low tier, the report must include
  `tcp_reverse_window`, `tuic_stream_pending`, `tuic_tcp_stream`,
  `relay_remote_timing`, `terminal_late_remote_payload`, and server-side
  evidence.
- If `.33` shows `fail auth`, inspect time sync, sing-box service/config, and
  mini_vpn TUIC env/config before attributing throughput.

## Decision Tree After VPS

- `tuic_stream_starved + target_sender_stalled`, with clean local/QUIC pressure
  and healthy local TCP window samples: investigate TUIC server-to-client stream
  delivery and stream polling/receive-window behavior before touching close
  drain again.
- `target_sender_stalled` plus `tcp_reverse_window may_recv_false` or early
  inactive/can_send false before useful bytes: investigate local TCP
  ACK/receive-window/FIN behavior.
- `terminal_closed_late_payload` only after a closed local socket with useful
  bytes already delivered: treat it as terminal accounting, not the first loss
  point.
- Any new QUIC loss, TUN drops, service failure, or auth failure: classify that
  run separately and do not mix it with the clean lifecycle branch.
