# Knife14by Egress Flush Headroom Results

Date: 2026-07-06

## Commits And Artifacts

- Code commit: `ee28d0f` (`fix(knife14by): align tx queue egress flush headroom`)
- VPS bundle:
  `/tmp/mini_vpn/knife14by_flush_headroom_20260706/mvpn_knife14by_flush_headroom_usclient_suite_20260706_220250.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14by_flush_headroom_20260706_local/`

## Run Shape

- Client: `.27` (`43.172.75.27`), repo at `ee28d0fe`, clean worktree.
- Exit: `.33` (`43.153.32.33`), sing-box active and accepting TUIC traffic.
- Target: `.77` (`43.130.32.77`), iperf3 active on TCP `:5201`.
- Suite mode: `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
  `SERVER_EVIDENCE_CHECK=1`, `EXIT_TO_TARGET_IPERF_CHECK=1`, `MTU=1200`,
  `MINI_VPN_TCP_DIAG=1`.
- Startup config: `high=524288`, `low=131072`,
  `tx_queue_pause_high=917504`, `tx_queue_resume_high=524288`,
  `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`.

## Baselines

- `.27 -> .77` direct forward: `330/268 Mbit/s` sender/receiver.
- `.27 <- .77` direct reverse: `303/275 Mbit/s` sender/receiver.
- `.33 -> .77` direct forward: `307/255 Mbit/s` sender/receiver.
- `.33 <- .77` direct reverse: `325/296 Mbit/s` sender/receiver.
- `.33` current-window evidence showed TUIC inbound and direct outbound lines
  for `.77:5201`. No current-window TUIC `fail auth` signal appeared.
- Exit and target clocks were NTP synchronized; client/exit epoch matched and
  target was one second ahead.

## Result

Reverse-first P1 improved only slightly and still failed the Knife14 throughput
goal:

- iperf sender: `30.0 Mbit/s`
- iperf receiver: `29.0 Mbit/s`
- shape: `low_average`
- interval profile: `overall_avg_mbps=29.001`, `tail_avg_mbps=14.158`,
  `tail_min_mbps=0.000`

The shape remained burst/idle: several one-second windows were strong
(`164`, `105`, `81.9 Mbit/s`), but seconds `8-12`, `13-16`, `17-20`, and
`27-30` contained zero-throughput gaps.

## Key Signals

- `downlink_backpressure`: `pause_edges=25`, `resume_edges=25`,
  `max_pending_bytes=0`, `max_tx_queue_bytes=975399`,
  `tx_queue_pause_high=917504`.
- `downlink_flush`: `tun_flush_deferred=25`, down from Knife14bx's `446`;
  `send_queue_max=916928`, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`.
- `tun_drops`: `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=37`.
- `quic`: no client-side loss, congestion, or stream/data blocking deltas.
- `terminal_pending_reap`: `0` events, `0` bytes.
- `pending_at_close`: `0` events, `0` bytes.
- `terminal_late_remote_payload`: `3908708` bytes across `142` events after
  the socket was already terminal closed.
- `egress_at_close`: `17960` bytes, classified `terminal_closed_no_send`;
  no send-capable close-drain candidate.
- TUIC data stream closed with `rx_bytes=112642300`, `reads=13452`,
  `max_read_gap_ms=4726`, and `max_pending_gap_ms=4726`.

## Interpretation

Knife14by validated one narrow part of the hypothesis: moving no-pending flush
deferral from the soft high watermark to the tx_queue hard cap reduced
`tun_flush_deferred` from `446` to `25`, and receiver throughput rose from
Knife14bx's `26.4 Mbit/s` to `29.0 Mbit/s`.

It did not pass. The same stop/go throughput shape remained, and a new small
TUN egress drop appeared (`tun_tx_dropped_delta=37`). That means the previous
soft-high flush gate was real local cadence friction, but opening it all the
way to the hard cap can push the TUN/qdisc edge. The active branch is still
mini_vpn local downlink egress cadence/capacity matching, not stale pool,
iperf3, sing-box, QUIC loss, terminal pending, or close/reap cleanup.

## Next Candidate Plan

Stop before another behavior edit. The next candidate should be TDD-first and
small:

1. Keep app-owned pending behavior strict at the existing high/low guard.
2. Keep tx_queue remote-read pause/resume headroom from Knife14bx.
3. Replace the all-or-nothing no-pending flush threshold with a bounded
   intermediate or adaptive TUN-capacity guard: more headroom than the soft
   high, but less aggressive than flushing up to the hard cap when recent
   pressure/drop evidence says the TUN side is saturated.
4. Add deterministic tests for no-pending flush at soft high, at the new
   intermediate/adaptive threshold, and after recent drop feedback.
5. Re-run the same reverse-first P1 suite and require: stable improvement
   beyond `29.0 Mbit/s`, `tun_flush_deferred` remains low, `tun_tx_dropped_delta`
   returns to zero or is explicitly bounded, and pending/close/reap signals
   remain clean.

Current overall Knife14 progress estimate: about `85%`. The stage advanced
because it removed one hidden soft gate and narrowed the root, but it cannot
claim the high-confidence range until a bounded egress-capacity rule improves
throughput without reintroducing TUN drops.
