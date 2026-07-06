# Knife14bz Bounded Egress Flush Plan

Date: 2026-07-06

## Grounding

Knife14by proved the previous no-pending flush gate at the soft high watermark
was too strict: `tun_flush_deferred` fell from `446` to `25` after moving that
gate to the tx_queue hard cap. It also proved the hard cap is too permissive:
reverse-first P1 stayed `low_average` at `29.0 Mbit/s`, and a new
`tun_tx_dropped_delta=37` appeared while tx_queue pressure reached `975399B`
against `tx_queue_pause_high=917504B`.

The active branch remains mini_vpn local downlink egress cadence/capacity
matching. The clean exclusions still hold: stale pool, iperf3, sing-box,
external path, QUIC loss/congestion/blocking, app-owned pending, close/reap,
and terminal pending are not the next target.

## Stage Goal

Solve the soft-threshold problem in core code by introducing a bounded
no-pending flush threshold between the app-pending soft high and the tx_queue
remote-read hard cap.

## Non-goals

- Do not change app-owned pending high/low semantics.
- Do not change tx_queue remote-read pause/resume thresholds.
- Do not tune TUN queue length, stale pools, iperf3, sing-box, or QUIC.
- Do not put VPS secrets, env dumps, or noisy logs into repository docs.

## Planned Behavior

For the default `high=524288`, `low=131072`:

- app-owned pending remains strict at `524288B`;
- no-pending immediate flush defers at `720896B`;
- tx_queue-only remote-read pause still starts at `917504B`;
- tx_queue-only remote-read resume still uses `524288B`.

The midpoint gives more flush headroom than the rejected soft-high gate, while
not pushing all the way to the hard cap that produced TUN drops.

## TDD Plan

1. Add a RED test showing no-pending flush is allowed above soft high but
   deferred at the bounded midpoint before the hard cap.
2. Keep the pending-backlog test that defers at soft high.
3. Update the Knife14by hard-cap-only test to assert the bounded midpoint
   instead of allowing nearly hard-cap flush.
4. Add or update diagnostic coverage so logs expose the new flush threshold.
5. Run focused pacer/backpressure/TUN feedback tests before broader gates.

## Acceptance

Local:

- Focused RED fails before the implementation and passes after it.
- Existing app pending, remote-read tx_queue headroom, TUN feedback, and close
  lifecycle tests stay green.
- `cargo test --lib`, harness tests, suite self-tests, release build, clippy,
  shell syntax checks, and `git diff --check` pass.

VPS:

- Re-run the same reverse-first P1 `.27 -> .33 -> .77` suite.
- Require receiver throughput to improve beyond `29.0 Mbit/s`, keep
  `tun_flush_deferred` materially below Knife14bx's `446`, and avoid new
  unbounded `tun_tx_dropped_delta`.
- Pending/close/reap accounting must remain clean.
