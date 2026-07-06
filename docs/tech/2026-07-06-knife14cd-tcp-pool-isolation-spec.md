# Knife14cd TCP Pool Isolation Spec

Date: 2026-07-06

## Grounding

Knife14cc commit `7d1f47f` passed local gates but failed two VPS acceptance
runs with pool=1. Both runs had healthy `.27/.33 -> .77` direct reverse
baselines, clean QUIC loss/congestion/blocking deltas, zero TUN drops, and
clean pending/reap accounting. The active failure was TUIC TCP data-stream
starvation on the same QUIC connection as the iperf reverse control stream.

An A/B rerun of the same binary with `MINI_VPN_TUIC_TCP_POOL=2` changed the
runtime shape from no-data to low-average:

- `tcp_pool: opens=2 conns=0,1`
- data stream `first_rx_ms=3`
- data stream `data_rx_bytes_max=101247460`
- reverse receiver `25.9 Mbit/s`

This did not finish Knife14. It only moved the bottleneck back to visible local
egress/drop/backlog pressure (`tun_tx_dropped_delta=541`,
`pending_at_close=553066`, `egress_at_close=892928`).

## Stage Goal

Make TUIC TCP pool isolation the default:

- product default `MINI_VPN_TUIC_TCP_POOL` behavior becomes pool=2;
- the US-client suite also defaults to pool=2 so it does not override product
  behavior back to pool=1;
- explicit `MINI_VPN_TUIC_TCP_POOL=1` remains valid for single-connection A/B,
  constrained exits, or regression reproduction; and
- parsing still rejects empty/invalid values to the default and clamps `0` to a
  non-empty minimum pool.

## Invariants

- Do not store TUIC credentials or VPS passwords in docs, logs, learnings, or
  code.
- Do not change UDP/health connection ownership: primary connection remains the
  primary UDP/health path.
- Do not add more than pool=2 by default from this evidence; the A/B proved
  isolation from `conn=0`, not that higher pool counts improve throughput.
- Do not re-open stale pool, iperf3, sing-box, egress pacer, or time-sync
  branches unless new evidence appears.

## Non-goals

- This stage does not solve the remaining local TUN egress/drop pressure.
- This stage does not tune queue thresholds or drain-credit values.
- This stage does not change sing-box server configuration.
- This stage does not claim relay-writer `flush()` fixes TUIC. In quinn 0.10.2,
  `SendStream::poll_flush` is a no-op, so the flush change is a generic
  AsyncWrite semantic safeguard rather than the expected VPS fix.

## Acceptance

Local:

- TDD proves default config uses pool=2.
- TDD proves explicit pool=1 remains valid and `0` clamps to 1.
- Existing relay writer tests remain green.
- Suite self-test and parser/shell checks remain green.
- Full local regression gates pass before commit.

VPS:

- `.27` runs the current branch with the suite default, without manually forcing
  `MINI_VPN_TUIC_TCP_POOL=2`.
- Startup/report evidence shows `TUIC TCP connection pool=2` and
  `tcp_pool: ... conns=0,1` for the reverse-first P1 window.
- The clean reverse-first P1 must not return to `throughput_shape=no_data`.
- Data stream `first_rx_ms` should stay near immediate rather than the
  pool=1 repeat's `17880ms`.
- If failure remains, final summaries must clearly attribute it to
  egress/drop/backlog (`tun_tx_dropped_delta`, `pending_at_close`,
  `egress_at_close`, or related final metrics), not hidden terminal pending
  reap.
