# Knife14ap results - downlink flush observability acceptance

Date: 2026-07-04

## Run

- Commit: `27aa73f`
- Client: `.27` (`43.172.75.27`)
- Exit: `.33` (`43.153.32.33`, sing-box/TUIC)
- Target: `.77` (`43.130.32.77`, iperf3)
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14ap_flushdiag_default_bp512_128_tty2_usclient_suite_20260704_154215.tar.gz`
- Extracted:
  `/tmp/mini_vpn/knife14ap_flushdiag_154215/`

Active client settings:

- `MINI_VPN_TUIC_CC=bbr`
- `MINI_VPN_TUIC_TCP_POOL=1`
- `MINI_VPN_TUN_MTU=1200`
- `MINI_VPN_TCP_DIAG=1`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`
- `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`

Preflight direct paths were healthy:

- `.27 -> .77`: forward receiver `282 Mbit/s`, reverse receiver `262 Mbit/s`
- `.33 -> .77`: forward receiver `286 Mbit/s`, reverse receiver `284 Mbit/s`

## Clean Reverse-First Result

The new diagnostics were present and decisive, but the throughput acceptance
still failed.

- iperf: `24.2/22.0 Mbit/s`
- attribution: `local_tun_egress_drop+local_downlink_backpressure`
- downlink backpressure: `pause_edges=8`, `resume_edges=8`,
  `max_pending_bytes=584779`
- downlink flush:
  - `attempts=13399`
  - `no_send_capacity=98`
  - `send_slice_calls=13301`
  - `accepted_bytes=72320335`
  - `zero=0`
  - `errors=0`
  - `budget_limited=1165`
  - `max_accepted_bytes=65536`
  - `tun_flush_calls=11671`
  - `tun_flush_failures=0`
- TUN drops: `tun_tx_dropped_delta=366`
- QUIC: no loss/congestion delta and no inherited congestion

## Interpretation

Knife14ap falsifies the idea that reverse throughput is low because
`send_slice` is failing, returning zero, or TUN flush calls are erroring. In the
clean window, smoltcp accepted every downlink byte observed by the diagnostics,
and `tun_flush_tx_failures=0`.

The remaining clean-window limiter is local egress behavior after smoltcp
acceptance:

- pending repeatedly reaches the 512 KiB high watermark;
- global remote reads pause/resume many times;
- data is accepted into smoltcp and flushed without API errors;
- TUN/qdisc still reports egress drops;
- the iperf receive stream shows repeated zero-throughput seconds.

This points to local TUN/qdisc burst/queue interaction or missing egress
feedback after successful `send_slice`, not iperf3, sing-box, QUIC
loss/congestion, or `send_slice` failure.

## Polluted Windows

The standard forward P1 reached `201/190 Mbit/s` but created heavy QUIC
loss/congestion and local write pressure. Later reverse windows inherited that
state (`min_start_cwnd` as low as `8911` or `32476` with large starting
loss/congestion counters), so they should not be used to judge the clean
downlink path.

## Next Design Direction

Do not tune iperf3, sing-box, or QUIC first. The next stage should design a
local egress feedback mechanism around bytes accepted into smoltcp but not yet
safe to burst into the TUN/qdisc. Candidate branches:

1. Pace or budget `iface.poll + device.flush_tx` work for downlink-heavy dirty
   handles instead of repeatedly dumping accepted bytes as fast as possible.
2. Make downlink backpressure react to TUN TX drops or egress pressure, not only
   `downlink_pending`.
3. Test a controlled TUN queue/pacing A/B only as evidence, not as the product
   fix.
