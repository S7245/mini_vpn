# Knife14ao spec - promote validated downlink watermarks

Date: 2026-07-04

## Grounding

Knife14an added a per-flush downlink budget of 256 KiB, but the VPS acceptance
run at
`/tmp/mini_vpn/mvpn_knife14an_flush256_defaultqlen_tty_usclient_suite_20260704_141008.tar.gz`
still showed clean reverse-first P1 at only 23.5 Mbit/s receiver. The run was
not path-limited: `.27 -> .77` direct reverse was 263 Mbit/s receiver and
`.33 -> .77` direct reverse was 283 Mbit/s receiver. The key signal was local:
`max_pending_bytes=2145742`, `tun_tx_dropped_delta=594`, and attribution
`local_tun_egress_drop+local_downlink_backpressure`.

Knife14ao then ran a no-code A/B on the same commit with
`MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`,
`MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`, and the same 256 KiB flush
budget. Bundle:
`/tmp/mini_vpn/mvpn_knife14ao_bp512_128_flush256_defaultqlen_tty_usclient_suite_20260704_142413.tar.gz`.
The clean reverse-first P1 improved to 157 Mbit/s receiver, with no new QUIC
loss/congestion and pending bounded at `max_pending_bytes=589570`.

## Design Tree

1. Keep the old 2 MiB / 512 KiB defaults.
   - Rejected. Knife14an already falsified per-flush-only pacing; the old high
     watermark still allows a burst large enough to overload local TUN egress.
2. Promote the validated 512 KiB / 128 KiB watermarks to defaults.
   - Accepted. This is the smallest product change supported by a clean
     reverse-first A/B and keeps existing env overrides for other deployments.
3. Promote stricter 256 KiB / 64 KiB defaults.
   - Rejected for this stage. The attempted A/B failed during TUIC startup
     before an app-level server log, so it is not a throughput result.
4. Replace the fixed watermark policy with a dynamic local-egress algorithm.
   - Deferred. The current evidence justifies a narrow default update first.

## Goal

Change the built-in TCP downlink backpressure defaults to:

- high: 524288 bytes
- low: 131072 bytes

Also align the US-client suite defaults so normal VPS runs exercise the product
default instead of overwriting it with the old values.

## Non-Goals

- No change to the per-flush budget default.
- No change to TUN queue length, TUIC TCP pool, congestion control, MTU, or
  sing-box/iperf3 configuration.
- No dynamic watermark algorithm in this stage.
- No change to stale TUIC TCP pool slot behavior.

## Invariants

- Env overrides must continue to work for high/low watermarks.
- Invalid or zero env values must still fall back to defaults.
- If low is greater than or equal to high, parsing must repair it to a valid
  value below high.
- Downlink pending remains lossless; this stage only changes when upstream
  reads are paused/resumed.

## Acceptance

Local:

- A focused Rust test asserts the default high/low values.
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

VPS:

- A fresh `.27` run without explicit
  `MINI_VPN_DOWNLINK_BACKPRESSURE_{HIGH,LOW}_BYTES` overrides reports
  `high=524288B low=131072B`.
- Clean reverse-first P1 reaches the same class as the no-code A/B rather than
  the Knife14an 23.5 Mbit/s receiver result.
- The run must not be interpreted if `client-tun` fails before the iperf probe
  window, as happened in the stricter 256/64 KiB A/B attempt.
