# Knife14aq spec - pace TCP downlink egress after smoltcp acceptance

Date: 2026-07-04

## Grounding

Knife14ap (`27aa73f`) added downlink flush observability and ran from `.27`.
The clean reverse-first tunnel window still failed at `24.2/22.0 Mbit/s`, while
direct `.27 -> .77` and `.33 -> .77` reverse baselines were healthy. The clean
window had no QUIC loss/congestion delta, no inherited QUIC congestion,
`send_slice` accepted all observed downlink bytes, and TUN flush calls returned
success. The remaining signals were local: downlink backpressure edges, pending
reaching the high watermark, and `tun_tx_dropped_delta`.

Current code path:

1. `handle_remote_payload` appends relay bytes to `downlink_pending`.
2. `flush_downlink` pushes as much as possible into the smoltcp TCP tx buffer.
3. The same remote-payload event immediately calls `iface.poll` and
   `device.flush_tx`.
4. Timer ticks also call `iface.poll` and `device.flush_tx` every 5 ms.

This means bytes can be accepted into smoltcp and then burst toward the TUN/qdisc
from every remote payload event, before the existing pending-based backpressure
can see local egress pressure.

## Goal

Add a conservative, observable egress pacing gate for TCP downlink remote-payload
flushes:

- allow a bounded amount of immediate `iface.poll + flush_tx` work after remote
  payload acceptance;
- defer additional immediate remote-payload flushes in the same timer interval
  to the existing 5 ms timer poll/flush path;
- keep `send_slice` correctness and `downlink_pending` preservation unchanged;
- make deferred immediate flushes visible in diagnostics and harness summaries.

## Non-Goals

- Do not tune iperf3, sing-box, TUIC congestion control, or QUIC transport
  parameters in this stage.
- Do not change TUN tx queue length as the product fix.
- Do not read OS qdisc drop counters from the product hot path.
- Do not remove the timer poll/flush path or inbound-packet poll/flush path.
- Do not change downlink backpressure watermarks in this stage.

## Invariants

- `downlink_pending` still preserves bytes that smoltcp cannot accept.
- A skipped immediate remote-payload TUN flush must not drop bytes; timer
  poll/flush remains responsible for writing generated packets.
- `flush_downlink` must remain bounded by `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES`.
- Pacing must be configurable for A/B runs and default to a conservative value.
- Diagnostics must show whether pacing engaged, so a VPS result can distinguish
  "pacer active but still dropping" from "pacer never engaged".

## Acceptance

Local:

- Unit tests cover egress pacing budget behavior, disabled-immediate behavior,
  config parsing, and aggregate diagnostic formatting.
- `cargo test --lib downlink_egress`
- `cargo test --lib tcp_downlink`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

VPS:

- Run the `.27` suite with writable TTY from the beginning.
- Use clean reverse-first P1 as the primary signal.
- Require direct `.27 -> .77` and `.33 -> .77` reverse baselines to remain
  healthy.
- Inspect whether `downlink_flush` reports deferred immediate flushes.
- A good result reduces local TUN egress drops and backpressure oscillation
  without introducing QUIC loss/congestion or `send_slice` errors.
