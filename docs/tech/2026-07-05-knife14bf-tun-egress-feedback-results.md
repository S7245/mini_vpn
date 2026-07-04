# Knife14bf TUN Egress Feedback Results

Date: 2026-07-05

Code under test: `e661613 fix(knife14bf): add tun egress feedback gate`

## Goal

Validate whether a TUN-drop-aware global receive pause can move clean
reverse-first TCP P=1 out of the 10-20 Mbit/s class, while keeping terminal
pending, close, and reap accounting explicit.

## Local Gates

Passed before VPS acceptance:

- `cargo test --lib client_tun`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `git diff --check`

## VPS Acceptance

First run:

- Remote bundle:
  `/tmp/conn/mvpn_knife14bf_tun_feedback_usclient_suite_20260705_073147.tar.gz`
- Local bundle:
  `/tmp/mini_vpn/knife14bf_startup_fail_20260705_073147/mvpn_knife14bf_tun_feedback_usclient_suite_20260705_073147.tar.gz`
- Result: invalid for throughput. mini_vpn failed during TUIC startup with
  `tuic auth finish: sending stopped by peer: error 0`.
- Follow-up: `.33` sing-box was active and client/server credential hashes
  matched without printing secrets. Restarting sing-box cleared the startup
  failure.

Valid rerun:

- Remote report:
  `/tmp/conn/mvpn_knife14bf_tun_feedback_rerun_usclient_suite_20260705_073541.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14bf_tun_feedback_rerun_usclient_suite_20260705_073541.tar.gz`
- Local bundle:
  `/tmp/mini_vpn/knife14bf_tun_feedback_rerun_20260705_073541/mvpn_knife14bf_tun_feedback_rerun_usclient_suite_20260705_073541.tar.gz`
- Client commit on `.27`: `e661613`
- Direct `.27 -> .77` baseline: forward receiver `277 Mbit/s`, reverse
  receiver `275 Mbit/s`.
- Direct `.33 -> .77` baseline: forward receiver `261 Mbit/s`, reverse
  receiver `299 Mbit/s`.

## Clean Reverse-First P1

Probe:
`mvpn_knife14bf_tun_feedback_rerun_usclient_tunnel_mtu1200_reverse_first_p1_20260705_073541.md`

Throughput:

- iperf sender: `22.8 Mbit/s`
- iperf receiver: `21.6 Mbit/s`

Key signals:

- `downlink_backpressure: pause_edges=7 resume_edges=7`
- `downlink_backpressure max_pending_bytes=1108466`
- `downlink_backpressure max_tx_queue_bytes=1048576`
- `downlink_flush accepted_bytes=73348553 zero=0 errors=0`
- `downlink_flush tun_flush_failures=0 tun_flush_deferred=0`
- `terminal_pending_reap: events=1 bytes=1108466`
- `pending_at_close: events=1 bytes=1108466 terminal_events=1`
- `tun_drops: tun_tx_dropped_delta=336`
- `runtime_tun_egress: drop_events=1 drop_delta_total=336`
- `tun_egress_feedback: pause_edges=0 resume_edges=0 drop_events=0`
- `quic: max_lost_bytes_delta=0 max_congestion_events_delta=0`
- Attribution:
  `local_tun_egress_drop+local_downlink_backpressure+terminal_pending_reap+pending_at_close`

Important raw-log timing:

- The TUN drop sample in the clean window was observed with
  `dirty_handles=0`, `pending_total=0`, and `global_rx_paused=false`.
- Therefore the new feedback gate did not pause in the clean window; by the
  time Linux reported the drop delta, mini_vpn no longer saw current local
  downlink pressure for that sample.

## Other Windows

Standard P1 forward:

- `92.0 / 81.6 Mbit/s`
- Attribution:
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`

Standard P1 reverse:

- `21.9 / 20.6 Mbit/s`
- `downlink_backpressure: pause_edges=11 resume_edges=10`
- `terminal_pending_reap=0`
- `tun_tx_dropped_delta=349`
- `tun_egress_feedback=0`
- Attribution:
  `inherited_quic_congestion+local_tun_egress_drop+local_downlink_backpressure`

Full forward:

- `33.5 / 27.3 Mbit/s`
- Attribution:
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`

Full reverse:

- `23.5 / 21.5 Mbit/s`
- `downlink_backpressure: pause_edges=4 resume_edges=3`
- `terminal_pending_reap=0`
- `tun_tx_dropped_delta=404`
- Attribution:
  `inherited_quic_congestion+local_tun_egress_drop+local_downlink_backpressure`

The raw client log proves the new feedback path can trigger on VPS:

- `tcp-tun-egress-feedback paused=true reason=drop_delta ... pause_edges=1`
- Later `tcp-tun-egress-feedback paused=false reason=pressure_low ... resume_edges=1`

That event occurred after the standard reverse/full transition while there was
current local pressure, not in the clean reverse-first P1 attribution window.

## Decision

Knife14bf did not meet the throughput target.

It did validate the parser/control plumbing and proved that TUN-drop feedback
can fire under VPS pressure, but it did not explain or fix the primary clean
reverse-first P1 failure. The clean failure still has:

- healthy direct baselines;
- no clean-window QUIC loss/congestion;
- no TUN flush failures, zero sends, or send errors;
- repeated 1MiB tx-queue/downlink backpressure;
- terminal pending/reap around 1.1MiB at close;
- TUN drop sampling that arrives too late or too sparsely to drive the new
  sysfs-based feedback gate in the clean window.

Rejected next moves remain unchanged: do not continue stale pool, sing-box,
iperf3, blunt egress pacer, TUN queue length, or larger receive-window tuning
without new evidence.

## Next Stage Bias

The next patch should focus on local TCP/TUN drain cadence and event-timed
pressure accounting, not larger buffers. A useful next design tree should
distinguish these branches:

- smoltcp tx queue reaches 1MiB because the local TCP sender is not being
  polled/drained frequently enough under downlink bursts;
- the downlink pause/resume loop creates burst/idle delivery that collapses
  iperf receiver throughput even though total remote bytes are read;
- close/reap is accounting the final backlog correctly, but the backlog itself
  represents data that never had enough local drain opportunity before local
  TCP closure;
- sysfs TUN drop sampling is a lagging diagnostic and should not be the primary
  control signal for clean P1.

Acceptance for the next stage should require clean reverse-first P1 to improve
materially and show reduced ordinary `downlink_backpressure` and terminal
pending, not only reduced `tun_tx_dropped_delta`.
