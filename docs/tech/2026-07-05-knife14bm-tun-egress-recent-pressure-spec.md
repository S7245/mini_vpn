# Knife14bm TUN Egress Recent Pressure Spec

Date: 2026-07-05

## Stage Goal

Make TUN egress drop feedback classify drops that are sampled shortly after
local downlink pressure drains. Knife14bl showed that the data-plane can recover
to about `130 Mbit/s`, but the sampler saw `tun_tx_dropped_delta=2707` only
after `downlink_pressure_stats` had returned to zero, so feedback did not pause
`global_rx`.

## Non-Goals

- Do not change TUIC pool behavior, sing-box config, iperf3, QUIC congestion
  control, or VPS topology.
- Do not lower product-wide egress immediate defaults.
- Do not change close/reap policy in this stage.
- Do not make sysfs TUN drop sampling hotter than the existing 1s cadence.

## Invariants

- A positive TUN drop with current local pressure must still pause feedback.
- A positive TUN drop with no current pressure and no recent high pressure must
  remain counted but must not pause feedback.
- Recent-pressure attribution is only latched when local downlink pressure
  reaches the configured high watermark.
- The latch is scoped to the next feedback sample so stale pressure cannot
  explain unrelated future drops.
- Resume remains based on current pressure falling to or below low watermark.

## Acceptance

Local:

- Add a deterministic unit test for drop-after-pressure-drain classification.
- Keep existing TUN egress feedback, downlink, parser, and lib tests passing.
- Keep clippy and diff whitespace gates clean.

VPS:

- If TUN drops recur after recent local pressure, `tun_egress_feedback` should
  report a drop event and pause edge instead of staying at zero.
- Reverse-first P1 should retain or improve Knife14bl throughput while reducing
  tail collapse and terminal pending.
- If `terminal_pending_reap` remains near 1 MiB with feedback now firing, the
  next branch should inspect close-drain/reap timing rather than feedback
  attribution.
