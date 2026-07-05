# Knife14bm Recent Pressure Results

Date: 2026-07-05

## Code Under Test

- Commit: `3d06bea` (`fix(knife14bm): latch recent pressure for tun drops`)
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Bundle:
  `/tmp/mini_vpn/knife14bm_recent_pressure_20260705_154706/mvpn_knife14bm_recent_pressure_usclient_suite_20260705_154706.tar.gz`

## Result

Knife14bm did not pass acceptance and did not exercise the recent-pressure TUN
drop attribution branch.

- Direct `.27 -> .77` reverse baseline: about `301/274 Mbit/s`.
- Direct `.33 -> .77` reverse baseline: about `282/256 Mbit/s`.
- Tunnel reverse-first P1: `1.19/0.00 Mbit/s` sender/receiver.
- Target `.77` iperf3 journal also showed only `4.25 MBytes /
  1.19 Mbit/s` sent for the tunneled reverse test.

This is not the Knife14bl tail-collapse shape. In Knife14bl the tunnel reached
about `130 Mbit/s`, then later hit local pressure, TUN drops, and terminal
pending. In Knife14bm the data stream did not deliver useful bytes during the
30s iperf window.

## Key Signals

- `tun_drops: tun_tx_dropped_delta=0`
- `runtime_tun_egress: drop_events=0 drop_delta_total=0`
- `tun_egress_feedback: pause_edges=0 resume_edges=0 drop_events=0`
- `downlink_backpressure: pause_edges=0 resume_edges=0`
- `downlink_flush: remote_to_global_rx_bytes=4 send_queue_max=1`
- `terminal_pending_reap: events=0 bytes=0`
- `pending_at_close: terminal_bytes=0`
- `tuic_stream_pending: max_pending_gap_ms=37604 pending_polls_max=77`
- `tuic_stream_polling: max_poll_gap_ms=5000`
- `tuic-tcp-stream-first-rx ... stream=4 first_rx_ms=37616`
- `quic: max_lost_bytes_delta=0 max_congestion_events_delta=0`

The server evidence window showed current TUIC inbound and direct outbound
opens on `.33` for `.27 -> .77:5201`. There was no current TUIC fail-auth
signal. The sing-box tail still contained unrelated VLESS/REALITY scan noise.

## Interpretation

Knife14bm cannot validate or reject the recent-pressure latch because there was
no local downlink pressure and no TUN drop to classify.

The dominant evidence is stream/send-side starvation before local egress:

- The iperf receiver saw zero bytes for the entire 30s window.
- The target sender also stopped after `4.25 MiB`, which means the target was
  not freely sending hundreds of Mbit/s that mini_vpn silently lost.
- mini_vpn kept polling the data stream, but it had `rx_bytes=0` until about
  `37.6s`.
- QUIC loss/congestion, local send-slice failures, TUN drops, and terminal
  pending were quiet in the clean window.

The most plausible branch is receive-window or ACK-path starvation around the
tunneled reverse stream, not the TUN qdisc pressure branch targeted by
Knife14bm.

## Next Plan

Do not make another TUN feedback or egress-pacer change from this run. The next
coherent task should be a small evidence-tightening stage before new behavior
changes:

1. Add or reuse a bounded A/B that runs the same reverse-first P1 command
   against `09bb67c` and `3d06bea` in the same VPS window, with server evidence
   enabled, to distinguish code regression from run-to-run stream readiness
   variance.
2. Extend the parser/report only if needed to surface the target sender
   `Cwnd/Retr` and `.33` connection-close timing in the attribution summary.
3. If the A/B reproduces low target sender bytes with quiet local egress,
   pivot the next behavior stage to local ACK/receive-window progress
   instrumentation and tests.
4. If the A/B returns to the Knife14bl pressure shape, keep the existing
   Knife14bm latch and continue close-drain/TUN pressure cleanup from that
   evidence.

Because this repair run failed in a different shape, the next behavior-code
change should wait for explicit confirmation of the plan.
