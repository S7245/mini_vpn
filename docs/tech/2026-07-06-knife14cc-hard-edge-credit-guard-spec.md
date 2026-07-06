# Knife14cc Hard-Edge Credit Guard Spec

Date: 2026-07-06

## Grounding

Knife14cb restored clean reverse-first P1 to `152/152 Mbit/s` with clean
pending/reap accounting, but the same VPS bundle still showed
`tun_tx_dropped_delta=712` and a two-second zero-throughput gap. The first drop
arrived when local egress pressure reached `917504B`, which is the current hard
tx_queue pause threshold.

The remaining issue is not authentication, sing-box, iperf3, QUIC loss, stale
pool slots, or terminal pending. It is that observed-drain credit can still be
spent all the way to the hard pause edge.

## Stage Goal

Keep the progress-clocked accept model, but reserve a small derived guard below
the hard pause threshold:

- clean headroom stays unchanged;
- observed egress drain still grants bounded one-shot credit;
- credit may raise planned accepts above the clean threshold;
- credit must not plan `send_queue + accepted` to the hard pause threshold; and
- diagnostics must show when this hard-edge guard deferred bytes.

## Non-goals

- Do not change backpressure high/low defaults or add a new environment knob.
- Do not return to static threshold-only accept.
- Do not re-open stale pool, sing-box, iperf3, QUIC, TUN rx drain, or egress
  pacer branches unless new evidence appears.

## Acceptance

Local:

- RED/GREEN test proves observed drain credit stops below the hard pause edge.
- Existing egress-clock, close/reap, parser, harness, and clippy gates remain
  green.

VPS:

- Re-run the scoped reverse-first P1 suite.
- Preserve high reverse throughput while reducing hard-edge pressure:
  `send_queue_max` should stay below the hard pause threshold.
- Require final pending/reap to remain zero and final TUN drops to disappear or
  produce a narrower residual signal than Knife14cb.
