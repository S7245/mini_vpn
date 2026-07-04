# Knife14bc spec - tx-window scaled downlink backpressure

Date: 2026-07-04

## Grounding

Knife14ba ruled out TUIC first-byte delay and data-stream read gaps in the clean
reverse-first window. The decisive clean-window shape was:

- iperf receiver: `28.7 Mbit/s`;
- data stream first receive: `3ms`;
- data-stream max read gap: `<4s`;
- clean-window QUIC loss/congestion: `0`;
- TUN drops and TUN flush failures: `0`;
- `send_slice_accepted=107824373`, matching the useful receiver bytes;
- local socket then entered `Closed`, and only after that did another `542302`
  bytes accumulate as terminal pending.

The terminal pending is therefore tail accounting after the local app closed.
The earlier throughput limiter is the receive-window/backpressure loop: with a
1MiB local TCP tx buffer, global_rx pauses at the fixed `512KiB` high watermark.
On a VPS RTT path this fixed half-buffer watermark can cap the effective
downlink window into the same `10-30 Mbit/s` band being investigated.

## Goal

Make tx-queue-driven downlink backpressure scale with the configured local TCP
tx buffer when the operator has not explicitly set backpressure watermarks.

## Non-goals

- Do not change TUIC pool, sing-box, iperf3, TUN queue length, or egress pacing.
- Do not remove backpressure.
- Do not change close/reap behavior in this slice.
- Do not override explicitly configured backpressure watermarks.

## Invariants

- App-owned `downlink_pending` remains bounded by the same hysteresis mechanism.
- Explicit `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES` or
  `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES` values still win.
- If TCP tx buffer is at or below the legacy default high watermark, legacy
  defaults remain unchanged.
- If TCP tx buffer is larger, the default high watermark rises to the tx buffer
  size and the low watermark rises to one quarter of that tx buffer.
- Parser and diagnostic field names remain unchanged.

## Acceptance

Local:

- focused tests for adaptive default watermarks;
- existing downlink backpressure and parser self-tests pass;
- `cargo test --lib client_tun::tests::`;
- `scripts/knife14b-lowrtt-probe.sh --self-test`;
- `scripts/knife14b-usclient-tunnel-suite.sh --self-test`;
- `git diff --check`.

VPS after commit:

- scoped reverse-first P1 from `.27` with `.33/.77` service preflight;
- startup log should show higher default downlink high/low when the `.env`
  TCP tx buffer is larger than `512KiB`;
- clean reverse-first should either leave the `10-20 Mbit/s` band with quiet
  loss/drop signals, or expose a new limiter.
