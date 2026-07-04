# Knife14be results - 4MiB receive-window A/B

Date: 2026-07-05

## Scope

Validate whether the Knife14 clean reverse-first bottleneck is primarily local
TCP tx-buffer / receive-window capacity.

- code: `5f1cbfb`
- client VPS: `.27` / `43.172.75.27`
- exit VPS: `.33` / `43.153.32.33`
- target VPS: `.77` / `43.130.32.77`
- valid local bundle:
  `/tmp/mini_vpn/knife14be_tx4m_auto_20260705_065724/mvpn_knife14be_tx4m_auto_usclient_suite_20260705_065724.tar.gz`
- valid remote bundle:
  `/tmp/conn/mvpn_knife14be_tx4m_auto_usclient_suite_20260705_065724.tar.gz`

The earlier remote bundle
`/tmp/conn/mvpn_knife14be_tx4m_auto_usclient_suite_20260705_065544.tar.gz`
is invalid because the first run was started without a tool-level writable TTY
and was killed at the sudo prompt.

## Configuration

The A/B run changed only the local TCP tx buffer from the Knife14bd 1MiB value
to 4MiB:

- `MINI_VPN_TCP_RX_BUFFER_BYTES=1048576`
- `MINI_VPN_TCP_TX_BUFFER_BYTES=4194304`
- downlink backpressure env: `<auto>`
- startup backpressure: `high=4194304B low=1048576B`
- startup TCP buffers: `rx=1048576B tx=4194304B`

## Preflight

The service/path preflight was healthy:

- `.27 -> .77` direct forward/reverse 1s baselines were `277/288 Mbit/s`
  receiver-side.
- `.33 -> .77` direct forward/reverse 1s baselines were `278/284 Mbit/s`
  receiver-side.
- `.33` sing-box and `.77` iperf3 were active before the run.

## Clean Reverse-First P1

This is the decision window for the A/B.

Knife14be 4MiB result:

- iperf: `20.3 Mbit/s` sender, `18.9 Mbit/s` receiver.
- backpressure: `pause_edges=0`, `resume_edges=0`.
- `send_queue_max=3786786`, below the 4MiB high watermark.
- `send_capacity_min=4194304`, `send_capacity_max=4194304`.
- pending at close: `0`.
- terminal pending reap: `0`.
- TUN drops: `tun_tx_dropped_delta=27530`.
- runtime TUN egress drops: `drop_events=3`, `drop_delta_total=14946`,
  `max_delta=14167`.
- QUIC clean-window loss/congestion delta: `0`.
- TUN flush failures/deferred: `0/0`.
- `send_slice_zero=0`, `send_slice_errors=0`.
- data stream first read: `3ms`.
- data stream max read gap: `3820ms`.
- attribution: `local_tun_egress_drop`.

Knife14bd 1MiB comparison:

- iperf receiver: `26.2 Mbit/s`.
- clean-window TUN drop delta: `0`.
- `send_queue_max=1048576`.
- terminal pending reap: `1058416`.
- attribution:
  `local_downlink_backpressure+terminal_pending_reap+pending_at_close`.

## Decision

The 4MiB A/B falsified the simple receive-window-capacity hypothesis.

Increasing the local TCP tx buffer did not improve useful reverse throughput;
it reduced the clean receiver result from `26.2 Mbit/s` to `18.9 Mbit/s` and
introduced clean-window TUN egress drops. The larger local queue allowed a
larger burst into the TUN/qdisc path without hitting the new 4MiB backpressure
high watermark, so the limiter moved from visible backpressure/terminal pending
to TUN egress loss.

This is new evidence that the next root is local TCP/TUN drain cadence or
TUN-drop-aware feedback, not more tx-buffer capacity.

## Later Polluted Windows

The later standard probes are not the clean root-cause window, but they show the
risk of the 4MiB setting:

- standard forward P1: `16.5/10.8 Mbit/s`, attribution
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`.
- standard reverse P1: `18.3/16.6 Mbit/s`, inherited QUIC congestion,
  `tun_tx_dropped_delta=8545`, `pending_at_close=14017`.
- full reverse P1: `15.8/13.9 Mbit/s`, inherited QUIC congestion,
  terminal pending/reap scaled to `4219586B`.

## Next Step

Do not keep increasing TCP tx buffers.

The next behavior slice should be a small, testable local TCP/TUN drain-cadence
stage:

1. Add focused observability/parser coverage that distinguishes:
   - tx queue near high watermark without TUN drops;
   - tx queue below high watermark with TUN drops;
   - terminal pending after local close.
2. Add a bounded feedback rule that prevents large TUN/qdisc bursts from being
   hidden by a large smoltcp tx buffer. This may touch `src/client_tun.rs`,
   parser scripts, and acceptance scripts; it should not be limited to one file.
3. Validate locally with deterministic accounting tests and parser fixtures
   before the next VPS run.

This should be treated as a new evidence-based direction, distinct from the
earlier rejected blunt egress-pacer tuning.
