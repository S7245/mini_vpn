# Knife14at results - runtime TUN egress feedback

Date: 2026-07-04

## Tested commit

- Code commit: `e8a7c41`
- Stage: Knife14at runtime TUN egress feedback
- Client VPS: `.27`
- Exit VPS: `.33`
- Target VPS: `.77`
- Remote report:
  `/tmp/conn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_191427.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_191427.tar.gz`
- Local bundle copy:
  `/tmp/mini_vpn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_191427.tar.gz`
- Local extracted directory:
  `/tmp/mini_vpn/knife14at_tun_egress_feedback_191427/`

## Preflight

- `.33` sing-box was active.
- `.77` iperf3 was active.
- `.27` was fast-forwarded to `e8a7c41`.
- The first suite attempt did not enter data-plane testing because the shell had
  not loaded the local TUIC environment. The successful run sourced the existing
  VPS-local `.env` without printing or storing secret values.
- Direct `.27 -> .77` one-second baselines were healthy:
  - forward receiver: `283 Mbit/s`
  - reverse receiver: `277 Mbit/s`

## Primary reverse-first result

The scoped reverse-first P1 window did not reproduce Knife14as low reverse
throughput:

- iperf sender: `185.000 Mbit/s`
- iperf receiver: `183.000 Mbit/s`
- `terminal_pending_reap: events=0 bytes=0`
- `relay_late_remote: post_finish_bytes=0 post_finish_reads=0`
- `local_write_pressure: events=0`
- `global_rx_pressure: events=0`
- QUIC loss/congestion deltas: `0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `tun_flush_deferred=0`
- `tun_tx_dropped_delta=376`
- `runtime_tun_egress: samples=6 drop_events=2 drop_delta_total=376 max_delta=318 unavailable=0 resets=0`
- attribution: `local_tun_egress_drop+local_downlink_backpressure`

This proves the new runtime sampler is wired correctly: process-local
`tcp-tun-egress` drop deltas matched the probe-level `ip -s link` delta in the
clean reverse-first window.

It also falsifies a stronger assumption from Knife14as: TUN egress drops plus
downlink backpressure are real, but they are not by themselves sufficient to
explain the prior `22.2 Mbit/s` reverse receiver result. The same branch reached
`183 Mbit/s` in this run while still observing TUN egress drops and downlink
backpressure.

## Standard P1 and full sweep

The normal forward-first P1 showed a different failure branch:

- forward P1 receiver: `2.060 Mbit/s`
- attribution:
  `quic_loss_congestion+local_write_pressure+local_tun_egress_drop`
- local write pressure: `12` events, max wait `4006.496 ms`
- QUIC loss/congestion:
  `max_lost_bytes_delta=5235906`,
  `max_congestion_events_delta=3559`

The following reverse P1 recovered high reverse throughput, but inherited the
forward-induced QUIC congestion state:

- reverse P1 receiver: `184.000 Mbit/s`
- `terminal_pending_reap=0`
- `local_write_pressure=0`
- `runtime_tun_egress drop_delta_total=6382`
- attribution:
  `inherited_quic_congestion+local_tun_egress_drop+local_downlink_backpressure`

The full sweep repeated the same shape:

- full forward receiver: `5.520 Mbit/s`
- full reverse receiver: `180.000 Mbit/s`
- full reverse `terminal_pending_reap=0`
- full reverse `runtime_tun_egress drop_delta_total=6595`

## Interpretation

Knife14at achieved its observability goal:

- `tcp-tun-egress` appears in mini_vpn process logs.
- The parser reports `runtime_tun_egress`.
- Runtime deltas align with probe-level TUN drop accounting.
- Unavailable/reset paths were not observed in the Linux VPS run.

The result changes the root-cause tree:

- The Knife14as terminal-pending branch remains closed for the active window.
- Runtime TUN egress drops are real and correlate with downlink/backpressure
  samples, but they do not explain low reverse throughput alone.
- Forward/uplink collapse is dominated by QUIC loss/congestion plus local write
  pressure and should not be debugged as a downlink terminal-pending problem.
- Reverse throughput can remain high even with local TUN egress drops, so the
  next stage should not keep tuning TUN queue length, downlink watermarks, or
  close-drain logic as if those were the sole reverse limiter.

## Next recommendation

Before another behavior change, do a short architecture review that separates:

1. TCP uplink write pressure and QUIC congestion recovery after forward traffic.
2. TCP downlink egress feedback and product-safe pacing/shaping.
3. Test sequencing effects, especially inherited congestion from a preceding
   forward probe.

The next code slice should be chosen from that review, not by another suffix
tuning pass on downlink pacing or terminal pending.
