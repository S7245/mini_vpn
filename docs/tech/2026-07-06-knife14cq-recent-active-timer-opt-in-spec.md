# Knife14cq Recent-Active Timer Opt-In Spec

Date: 2026-07-06

## Goal

Remove Knife14cp recent-active timer ACK drain from the product default path
after VPS evidence showed it engaged but regressed reverse-first throughput.
Keep it available only as an explicit A/B diagnostic knob.

## Evidence

Knife14cp valid VPS run:

- reverse-first P1 regressed to `19.2/18.0 Mbit/s`
- `timer_active_flow_attempts=1321`, proving the new timer path ran
- local pressure stayed clean: `pause_edges=0`, `tun_tx_dropped_delta=0`,
  `drop_delta_total=0`
- pending/close/reap and QUIC health stayed clean
- attribution remained `no_pressure_signal`

## Non-Goals

- Do not tune the timer from `250ms` to another default value.
- Do not add another below-pressure ACK-drain timer variant.
- Do not change stale pool, iperf3, sing-box, egress pacer, TUN qdisc length,
  or receive-window sizing from this evidence.

## Invariants

- Default runtime behavior must match the cleaner Knife14co path: active-flow
  drain remains payload-triggered, not background timer-triggered.
- Recent-active timer drain may only run when explicitly configured with
  `MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS>0`.
- The explicit timer value is bounded so the A/B path cannot become an
  unbounded hot-path TUN RX scan.
- Diagnostics may keep `timer_active_flow_attempts` so future A/B runs can
  prove whether the path was active.

## Acceptance

- Local tests prove the default timer setting is `0`.
- Local tests prove recent-active timer budget returns `0` by default and a
  bounded active-flow budget only when explicitly enabled.
- Focused TUN RX, backpressure, debt, close, and reap regressions pass.
- Broad gates pass before any further VPS acceptance.
