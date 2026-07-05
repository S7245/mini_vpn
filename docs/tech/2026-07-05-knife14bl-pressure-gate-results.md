# Knife14bl Pressure Gate Results

Date: 2026-07-05

## Code Under Test

- Commit: `09bb67c` (`fix(knife14bl): gate immediate downlink flush on pressure`)
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Bundle:
  `/tmp/mini_vpn/knife14bl_pressure_gate_20260705_152416/mvpn_knife14bl_pressure_gate_usclient_suite_20260705_152416.tar.gz`

## Result

Knife14bl did not pass final Knife14 acceptance, but it is a material behavior
improvement over Knife14bk.

- Direct `.27 -> .77` reverse baseline: about `308/281 Mbit/s`.
- Direct `.33 -> .77` reverse baseline: about `310/282 Mbit/s`.
- Tunnel reverse-first P1: `131/130 Mbit/s` sender/receiver.
- Knife14bk comparison: `18.8/17.0 Mbit/s`.

The run therefore escaped the old 10-20 Mbit/s band, but it was not stable: the
last eight seconds collapsed back to about `11-24 Mbit/s`, and close/reap still
reported terminal pending.

## Key Signals

- `downlink_backpressure: pause_edges=143 resume_edges=143`
- `downlink_flush: attempts=34351 no_send_capacity=11 may_recv_false=0
  tun_flush_deferred=142 pending_high=65536`
- `tun_drops: tun_tx_dropped_delta=2707`
- `runtime_tun_egress: drop_events=1 drop_delta_total=2707`
- `tun_egress_feedback: drop_events=0`
- `terminal_pending_reap: events=1 bytes=1048938`
- `pending_at_close: terminal_bytes=1048938`
- `tuic_stream_pending: data_pending_gap_max_ms=193`
- `tuic_stream_polling: data_poll_gap_max_ms=193`
- `quic: max_lost_bytes_delta=0 max_congestion_events_delta=0`

The server evidence artifact showed `.77` iperf3 sender throughput matched the
tunnel result. `.33` had current TUIC inbound/direct outbound lines for the
probe and no current TUIC fail-auth evidence. Older VLESS/REALITY scan noise was
still present in the tail-bounded sing-box log capture.

## Interpretation

The pressure-aware immediate flush gate helped: the clean data stream was no
longer starved and reverse throughput rose to about `130 Mbit/s`.

The remaining failure is still local lifecycle/egress pressure:

- The smoltcp send queue repeatedly reached the high watermark.
- TUN tx drops were sampled after the local pressure had already drained, so the
  feedback path did not classify or pause on the drop (`runtime_tun_egress`
  saw it, `tun_egress_feedback` did not).
- The final close had a terminal pending reap of about 1 MiB, so pending/close
  accounting is not yet clean enough for Knife14 acceptance.

## Next Plan

Do not re-open stale pool, sing-box auth/time/config, iperf3, QUIC, or blunt
egress default tuning from this run.

The next code step should be scoped to one of these local branches:

1. Add a short recent-pressure latch to TUN egress feedback so a drop sampled
   just after pressure drains is still attributed and can pause global_rx.
2. Add a deterministic test for drop-after-pressure-drain classification.
3. Consider a follow-up bounded TUN flush or lower immediate-flush pressure
   threshold only if the latch still leaves qdisc drops and tail collapse.

Because this VPS repair run failed final acceptance, apply the next behavior
change only after explicit confirmation of this plan.
