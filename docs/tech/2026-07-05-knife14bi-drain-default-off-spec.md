# Knife14bi Drain Default-Off Spec

Date: 2026-07-05

## Stage Goal

Knife14bi removes the Knife14bg regression trigger by making the product and
VPS suite default `MINI_VPN_TUN_RX_DRAIN_BUDGET` value `0`.

The opportunistic TUN RX drain path remains available as an explicit A/B knob,
but it must no longer run by default.

## Grounding

Knife14bh tested commit `7e33d44` with two scoped reverse-first P1 VPS runs.

- `MINI_VPN_TUN_RX_DRAIN_BUDGET=8`: receiver `0.035 Mbit/s`, data stream first
  useful RX about `24.5s`, only `209758B` received.
- `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`: receiver `24.5 Mbit/s`, data stream first
  RX `4ms`, about `99MB` received.

Both runs had healthy direct baselines and clean-window QUIC loss/congestion
remained zero. Therefore the default drain budget `8` is a local regression
trigger.

## Non-Goals

- Do not optimize the remaining drain-disabled local TUN egress/downlink
  backpressure branch in this patch.
- Do not remove the drain code or parser metrics.
- Do not change close/reap grace, TCP socket buffers, QUIC windows, sing-box,
  iperf3, or pool behavior.

## Invariants

- Default runtime config uses `tun_rx_drain_budget=0`.
- Suite default and help text report `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`.
- Operators can still explicitly set `MINI_VPN_TUN_RX_DRAIN_BUDGET=8` or any
  valid `0..=64` value for A/B.
- Parser summaries keep reporting `tcp-tun-rx-drain`.

## Acceptance

Local:

- focused parse/default tests prove default `0` and explicit `8`;
- suite self-test proves help/default drift is caught;
- low-RTT parser self-test still passes;
- shell syntax, `git diff --check`, client-tun tests, and clippy pass.

VPS:

- run default scoped reverse-first P1 without overriding
  `MINI_VPN_TUN_RX_DRAIN_BUDGET`;
- startup must show `TUN RX drain budget: 0`;
- summary must show `tun_rx_drain: attempts=0 packets=0`;
- receiver should match the Knife14bh drain0 shape rather than the drain=8
  collapse.
