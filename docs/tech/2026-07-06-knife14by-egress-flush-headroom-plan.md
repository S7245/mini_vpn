# Knife14by Egress Flush Headroom Plan

Date: 2026-07-06

## Grounding

Knife14bx changed remote-read backpressure so tx_queue-only pressure no longer
pauses `global_rx` at the conservative soft high watermark. The VPS acceptance
partially improved reverse-first P1 from `19.8/18.9 Mbit/s` to
`27.4/26.4 Mbit/s`, and downlink backpressure churn dropped from `51/51` to
`28/28`.

The run still failed with `throughput_shape=low_average`. The new discriminator
was not external service health, QUIC loss, TUN drops, send-slice failure, or
terminal pending reap. Instead, `tun_flush_deferred=446` while app pending
stayed `0`: remote-read backpressure now uses the tx_queue hard cap
(`917504` bytes by default), but `DownlinkEgressPacer::allow_remote_payload_flush`
still defers immediate TUN flushes at the soft high (`524288` bytes).

## Stage Goal

Align tx_queue-only immediate flush deferral with the tx_queue hard cap added
in Knife14bx, so the local downlink path does not keep a second soft-high
cadence gate after remote reads have already been allowed to continue.

## Non-goals

- Do not change stale pool, sing-box, iperf3, QUIC congestion, TUN qlen, or
  connection pool behavior.
- Do not change app-owned pending acceptance or the high/low pending
  backpressure guard.
- Do not change TUN drop feedback attribution.
- Do not store secrets or noisy raw logs in repository docs.

## Planned Behavior

- If `pending_bytes == 0`, `allow_remote_payload_flush` defers at
  `tx_queue_pause_threshold(cfg)` instead of `cfg.high_bytes`.
- If `pending_bytes > 0`, keep the existing stricter behavior: non-empty app
  pending can force a flush below the soft high, but a send queue already at
  soft high still defers.
- The immediate byte budget behavior remains unchanged.

## TDD Plan

1. Add a focused RED test proving no-pending tx_queue pressure at soft high
   still permits immediate flush.
2. In the same test, prove no-pending tx_queue pressure at the hard cap still
   defers and debits accepted bytes.
3. Keep the existing pending-backlog tests unchanged, especially the test that
   defers pending flush at soft high.
4. Run focused pacer/backpressure tests before broader local gates.

## Acceptance

Local acceptance:

- Focused new pacer test fails before the implementation and passes after it.
- Existing pending-backlog, backpressure, TUN feedback, and close lifecycle
  tests stay green.
- `cargo test --lib`, harness tests, suite self-tests, shell syntax checks,
  release build, clippy with harness, and `git diff --check` pass.

VPS acceptance:

- Run the same `.27 -> .33 -> .77` reverse-first P1 suite with server evidence.
- Compare against Knife14bx:
  - reverse throughput should leave the `10-20 Mbit/s` tier and ideally improve
    beyond `26.4 Mbit/s`;
  - `tun_flush_deferred` should fall materially;
  - no TUN drops, send-slice errors, QUIC loss/congestion/blocking, terminal
    pending reap, or `.33` TUIC auth failure should appear.
