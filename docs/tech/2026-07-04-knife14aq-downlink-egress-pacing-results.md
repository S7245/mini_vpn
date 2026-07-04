# Knife14aq results - downlink egress pacing rejected as default

Date: 2026-07-04

Code commit: `212ce26`

Local bundle:
`/tmp/mini_vpn/mvpn_knife14aq_egress_pacing_default_usclient_suite_20260704_160317.tar.gz`

Extracted bundle:
`/tmp/mini_vpn/knife14aq_egress_160317/`

Remote bundle:
`/tmp/conn/mvpn_knife14aq_egress_pacing_default_usclient_suite_20260704_160317.tar.gz`

## Verdict

Knife14aq failed VPS acceptance.

The new `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=65536` pacer engaged and
reduced clean reverse-first TUN egress drops compared with Knife14ap, but it
also reduced tunnel throughput and exposed a lifecycle boundary where useful
downlink bytes can still be pending when the local TCP socket has already moved
to `Closed`/`can_send=false`.

This is a useful discriminator, not a shippable default.

## Baselines

The suite used direct preflight checks before starting the tunnel:

- `.27 -> .77` forward receiver: `279 Mbit/s`
- `.27 -> .77` reverse receiver: `295 Mbit/s`
- `.33 -> .77` forward receiver: `282 Mbit/s`
- `.33 -> .77` reverse receiver: `283 Mbit/s`

The failure is therefore not explained by iperf3 being generally unhealthy, the
target host being saturated, or the exit-to-target path being slow.

## Primary Signal

Clean reverse-first P1 is the primary acceptance window because it avoids
inherited QUIC loss/congestion from a preceding forward run.

Knife14ap clean reverse-first, before egress pacing:

- receiver: `22.0 Mbit/s`
- attribution: `local_tun_egress_drop+local_downlink_backpressure`
- downlink backpressure: `pause_edges=8`, `resume_edges=8`,
  `max_pending_bytes=584779`
- downlink flush: `accepted_bytes=72320335`, `send_slice_zero=0`,
  `send_slice_errors=0`, `tun_flush_failures=0`
- TUN drops: `tun_tx_dropped_delta=366`
- QUIC clean-window loss/congestion delta: `0`

Knife14aq clean reverse-first, with `65536B/tick` immediate egress budget:

- receiver: `13.8 Mbit/s`
- attribution: `local_tun_egress_drop+local_downlink_backpressure`
- downlink backpressure: `pause_edges=12`, `resume_edges=11`,
  `max_pending_bytes=589805`
- downlink flush: `accepted_bytes=48073187`, `send_slice_zero=0`,
  `send_slice_errors=0`, `tun_flush_failures=0`,
  `tun_flush_deferred=1057`
- TUN drops: `tun_tx_dropped_delta=65`
- QUIC clean-window loss/congestion delta: `0`

Interpretation:

- The pacer is active: `tun_flush_deferred=1057`.
- It reduces TUN drop count in the clean window: `366 -> 65`.
- It worsens user-visible throughput: `22.0 -> 13.8 Mbit/s` receiver.
- It does not remove downlink backpressure oscillation.

## Close Boundary Signal

Immediately after the clean reverse-first window, the logs show a reaped relay
slot with useful downlink bytes still pending:

```text
reason=dead_slot_reap state=Relaying pending=576827 pending_high=589805
remote_to_global_rx_bytes=53272660 send_slice_accepted=52695833
no_send_capacity=257 tcp_state=Closed active=false can_send=false can_recv=false
```

Later reverse windows also closed with large pending values, for example
`pending=534136` and `pending=538055` on `uplink_channel_closed`.

This means the current remote-payload immediate-flush deferral is too blunt.
It can reduce how aggressively mini_vpn writes to TUN/qdisc, but it also delays
egress progress enough that connection close/reap can meet non-empty downlink
pending. Lifecycle correctness has priority over throughput tuning, so the
current default must not be treated as accepted.

## Secondary Windows

Post-forward forward and reverse samples are secondary because they include
local write pressure, QUIC loss/congestion, or inherited congestion:

- Standard forward P1: receiver `3.56 Mbit/s`, attribution
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`
- Standard reverse P1: receiver `25.0 Mbit/s`, attribution
  `local_tun_egress_drop+local_downlink_backpressure`
- Full forward P1: receiver `4.72 Mbit/s`, attribution
  `quic_loss_congestion+local_write_pressure+local_tun_egress_drop`
- Full reverse P1: receiver `26.2 Mbit/s`, attribution
  `local_tun_egress_drop+local_downlink_backpressure`

These windows confirm the remaining problem is still local data-plane pressure,
but they are not the clean acceptance signal.

## Next Design Constraints

The next stage should not keep the Knife14aq `65536B/tick` pacer as a default
throughput fix.

Required direction for Knife14ar:

- make the blunt pacer default-off or otherwise preserve the old immediate
  flush behavior until a close-safe design passes acceptance;
- add deterministic tests for "accepted or pending downlink bytes must not be
  silently stranded across close/reap";
- force drain/progress around local close, remote EOF, FIN, or reap boundaries
  before releasing a relay with non-empty pending bytes;
- prefer pacing the actual TUN/qdisc write cadence or flush budget over
  skipping immediate `iface.poll` wholesale;
- keep `tun_flush_deferred`, pending-at-close, and backpressure diagnostics in
  every acceptance report.
