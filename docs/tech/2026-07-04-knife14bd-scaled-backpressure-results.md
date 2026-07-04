# Knife14bd results - adaptive backpressure did not clear clean reverse-first

Date: 2026-07-04

## Scope

Validate Knife14bc/Knife14bd together on the US client path:

- code: `9578f5e`
- client VPS: `.27` / `43.172.75.27`
- exit VPS: `.33` / `43.153.32.33`
- target VPS: `.77` / `43.130.32.77`
- local bundle: `/tmp/mini_vpn/knife14bd_scaled_auto_20260704_234307/mvpn_knife14bd_scaled_auto_usclient_suite_20260704_234307.tar.gz`
- remote bundle: `/tmp/conn/mvpn_knife14bd_scaled_auto_usclient_suite_20260704_234307.tar.gz`

The run used the suite's `<auto>` downlink backpressure env values so the binary
could derive watermarks from `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576`.

## Preflight

The service/path preflight was healthy:

- `.27 -> .77` direct forward/reverse 1s baselines were `276/282 Mbit/s`
  receiver-side.
- `.33 -> .77` direct forward/reverse 1s baselines were `281/280 Mbit/s`
  receiver-side.
- The startup log confirmed adaptive watermarks:
  `high=1048576B low=262144B`.
- The local TCP socket buffers were `rx=1048576B tx=1048576B`.

## Clean Reverse-First P1

Clean reverse-first P1 remained below the desired Knife14 level:

- iperf: `27.8 Mbit/s` sender, `26.2 Mbit/s` receiver.
- QUIC loss/congestion delta: `0`.
- TUN drop delta: `0`.
- TUN flush failures/deferred: `0/0`.
- local/global write pressure: `0/0`.
- data stream first read: `3ms`.
- data stream max read gap: `3826ms`.
- data stream bytes: `104273376`.
- downlink backpressure: `pause_edges=11`, `resume_edges=11`.
- `send_queue_max=1048576`.
- `max_pending_bytes=1058416`.
- `terminal_pending_reap=1058416`.
- close class: `terminal_closed_no_send`.

The adaptive high watermark took effect, but the tx queue still hit the full
1MiB socket buffer repeatedly. The iperf per-second output remained burst/idle
instead of smoothly draining. The terminal pending tail scaled to roughly the
tx-buffer size rather than disappearing.

## Later Standard Reverse P1

The later standard reverse P1 is not a clean root-cause window:

- iperf: `21.2 Mbit/s` sender, `20.0 Mbit/s` receiver.
- inherited QUIC congestion was already present from the preceding forward P1:
  `max_start_lost_bytes=6921684`, `max_start_congestion_events=4748`,
  `min_start_cwnd=2993`.
- TUN egress drops appeared in that later window:
  `tun_tx_dropped_delta=7282`.
- attribution: `inherited_quic_congestion+local_tun_egress_drop+local_downlink_backpressure`.

This window is useful for proving the suite can identify pollution, but it
should not override the clean reverse-first conclusion.

## Conclusion

Knife14bd failed as a throughput fix. Raising default downlink backpressure from
the legacy fixed 512KiB high watermark to the 1MiB TCP tx buffer was necessary
for a valid test, but it was not sufficient to stabilize clean reverse-first
throughput.

The current clean evidence points to local TCP tx-buffer / receive-window sizing
or delivery cadence, not to:

- stale TUIC pool slots;
- iperf3 or target service health;
- sing-box service health;
- egress pacing;
- TUN syscall failure;
- clean-window TUN qdisc drops;
- clean-window QUIC congestion/loss;
- TUIC first-byte delay.

## Next Step

Before another code patch, run a scoped receive-window A/B:

1. Keep the same clean reverse-first-only suite shape.
2. Override `MINI_VPN_TCP_TX_BUFFER_BYTES=4194304`.
3. Let backpressure remain `<auto>` so high/low scale to the tx buffer.
4. Keep `MINI_VPN_TCP_RX_BUFFER_BYTES=1048576` unless a separate uplink test
   requires otherwise.
5. Accept the BDP hypothesis only if throughput improves materially while
   clean-window QUIC/TUN/pending signals stay bounded.

If 4MiB only scales terminal pending and does not improve the burst/idle shape,
stop tuning buffer sizes and inspect the local TCP/TUN drain cadence instead.
