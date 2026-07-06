# Knife14bz Bounded Egress Flush Results

Date: 2026-07-06

## Commits And Artifacts

- Code commit: `3d88212` (`fix(knife14bz): bound no-pending egress flush headroom`)
- VPS bundle:
  `/tmp/mini_vpn/knife14bz_bounded_flush_20260706/mvpn_knife14bz_bounded_flush_usclient_suite_20260706_221933.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14bz_bounded_flush_20260706_local/`

## Run Shape

- Client: `.27` (`43.172.75.27`), repo at `3d88212b`, clean worktree.
- Exit: `.33` (`43.153.32.33`), sing-box active and accepting TUIC traffic.
- Target: `.77` (`43.130.32.77`), iperf3 active on TCP `:5201`.
- Suite mode: `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
  `SERVER_EVIDENCE_CHECK=1`, `EXIT_TO_TARGET_IPERF_CHECK=1`,
  `DIRECT_IPERF_REVERSE_CHECK=1`, `MTU=1200`, `MINI_VPN_TCP_DIAG=1`.
- Startup config confirmed `tx_queue_flush_high=720896B`,
  `tx_queue_pause_high=917504B`, and `tx_queue_resume_high=524288B`.

## Baselines

- `.27 -> .77` direct forward: `323/279 Mbit/s` sender/receiver.
- `.27 <- .77` direct reverse: `313/280 Mbit/s` sender/receiver.
- `.33 -> .77` direct forward: `308/284 Mbit/s` sender/receiver.
- `.33 <- .77` direct reverse: `319/297 Mbit/s` sender/receiver.
- `.33` and `.77` reported NTP synchronized.
- `.33` current-window evidence showed TUIC inbound and direct outbound lines
  for `.77:5201`. No current-window TUIC `fail auth` signal appeared.

## Result

Reverse-first P1 regressed and failed the Knife14 throughput goal:

- iperf sender: `20.7 Mbit/s`
- iperf receiver: `19.5 Mbit/s`
- shape: `low_average`
- attribution: `local_tun_egress_drop`, `local_tun_egress_feedback`,
  `local_downlink_backpressure`, `terminal_late_remote_payload`,
  `terminal_closed_late_payload`, and `egress_at_close`

The interval profile stayed stop/go: seconds `0-1`, `2-3`, `9-10`,
`13-14`, `17-18`, `21-22`, and `28-29` delivered bursts, while many adjacent
seconds were `0.00 bits/sec`.

## Key Signals

- `downlink_backpressure`: `pause_edges=20`, `resume_edges=20`,
  `max_pending_bytes=0`, `max_tx_queue_bytes=983022`,
  `tx_queue_pause_high=917504`.
- `downlink_flush`: `tun_flush_deferred=162`, `send_queue_max=917489`,
  `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`.
- `tun_drops`: `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=340`.
- `tun_egress_feedback`: `pause_edges=1`, `resume_edges=1`,
  `drop_delta_total=340`, `max_pressure_bytes=983022`.
- `quic`: no client-side loss, congestion, stream/data blocking, or inherited
  low-cwnd signal.
- `terminal_pending_reap`: `0` events, `0` bytes.
- `pending_at_close`: `0` events, `0` bytes.
- `egress_at_close`: `23936` bytes, classified `terminal_closed_no_send`;
  no send-capable close-drain candidate.
- Close line: `pending=0`, `terminal_late_remote_payload_bytes=568695`,
  `terminal_late_remote_payload_events=57`, `tcp_state=Closed`,
  `can_send=false`.
- TUIC data stream closed with `rx_bytes=73943976`, `reads=4679`,
  `max_read_gap_ms=3826`, and `max_pending_gap_ms=3826`.

## Interpretation

Knife14bz disproved the static midpoint threshold hypothesis. The new
`tx_queue_flush_high=720896B` did take effect, but remote-read bursts still
pushed local send queue pressure up to the hard-pause region
(`917489-983022B`) before the TUN side drained cleanly. That produced more
flush deferral than Knife14by (`162` vs `25`), much larger TUN egress loss
(`340` vs `37`), and lower receiver throughput (`19.5` vs `29.0 Mbit/s`).

This means the remaining bug is not "find the right soft threshold number" in
the existing all-or-nothing gate. The active root is still mini_vpn local TCP
downlink egress cadence/capacity matching, but the next code shape should move
one layer earlier: limit or slice remote payload acceptance based on remaining
egress headroom before `send_slice` can overshoot the clean TUN capacity.

The clean exclusions still hold for this run: direct baselines were healthy,
`.33` TUIC auth was clean in the current window, QUIC loss/congestion/blocking
was zero, app pending stayed zero, send-slice errors stayed zero, and
pending/close/reap accounting did not hide a lost pending buffer.

## Next Candidate Plan

Stop before another behavior edit. The next stage should be TDD-first and
should target bounded remote payload acceptance, not another threshold-only
tweak:

1. Add a deterministic test where a remote payload larger than the remaining
   tx_queue clean headroom is only partially accepted, leaving the remainder
   pending or deferred without reporting a terminal loss.
2. Keep app-owned pending strict at `high`, and keep close/reap pending
   accounting unchanged.
3. Preserve the existing tx_queue diagnostics, but add explicit counters for
   headroom-limited accepts so the next VPS run can distinguish "bounded by
   design" from "lost or terminal pending".
4. Re-run the same reverse-first P1 suite only after local tests pass.
5. Acceptance should require throughput above the Knife14by `29.0 Mbit/s`
   receiver baseline, materially lower stop/go gaps, no unbounded TUN drop,
   and still-clean pending/close/reap signals.

Current overall Knife14 progress estimate: about `82%`. Confidence moved down
from Knife14by's `85%` because the midpoint threshold hypothesis failed, but
the root narrowed: the next candidate is a concrete core-code change at
remote-payload acceptance headroom rather than another external-service or
script branch.
