# Knife14ci Adaptive TUN RX ACK Drain Spec

Date: 2026-07-06

## Goal

Reduce reverse-first TCP burst/stall behavior by processing local TCP ACK and
window updates when downlink egress is near the credit edge.

Knife14ch repeat showed healthy `.27/.33/.77` baselines, no QUIC
loss/congestion/blocking deltas, no TUN drops, no global receive pressure, and
no final pending backlog, but reverse-first P1 stayed in the `10-20 Mbit/s`
class. The run also showed smoltcp `send_queue_max=892928`, credit-guard
limiting, and a `4249ms` remote read gap. With the product default
`MINI_VPN_TUN_RX_DRAIN_BUDGET=0`, the remote payload branch marks dirty downlink
work but does not opportunistically drain ready TUN RX packets, so local ACK and
window progress can wait for the next selected TUN RX event.

Knife14ci keeps explicit TUN RX drain disabled by default, but adds a derived
pressure-only adaptive pass:

- if the payload accepted bytes or still has pending downlink work;
- and the smoltcp send queue has reached the downlink egress credit threshold;
- and no explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET` override is configured;
- then drain a small bounded number of ready TUN RX packets immediately.

The adaptive budget is derived from the existing egress credit guard and TUN
MTU, then capped by a small hard maximum. This makes the behavior tied to the
same pressure model as downlink acceptance instead of a new static tuning knob.

## Non-Goals

- Do not change sing-box, iperf3, VPS service config, or TCP pool direction.
- Do not re-enable the rejected unconditional/opportunistic TUN RX drain path.
- Do not increase global receive windows or remote read-ahead.
- Do not tune TUN qdisc length or the egress pacer.
- Do not hide lifecycle loss: pending, close, terminal late payload, and reap
  metrics remain acceptance evidence.

## Invariants

- `MINI_VPN_TUN_RX_DRAIN_BUDGET>0` remains an explicit override and preserves
  the configured budget.
- Default `MINI_VPN_TUN_RX_DRAIN_BUDGET=0` remains idle below the credit edge.
- No adaptive drain occurs if a remote payload accepted no bytes and there is
  no per-socket downlink pending work.
- The adaptive budget is finite, MTU-derived, and capped.
- TUN RX drain diagnostics continue to report attempts, packets, TCP/DNS/UDP
  packet counts, exhausted budget, would-block, and errors.

## Acceptance

Local:

- Focused tests prove explicit override precedence, no-work/no-pressure
  suppression, adaptive enablement at the credit edge, and guard/MTU budget
  derivation.
- Existing pressure-credit, egress-clock, scripts, unit, full, harness, clippy,
  and diff-check gates pass.

VPS:

- Clean reverse-first P1 suite with required direct `.27 -> .77` and
  `.33 -> .77` baselines.
- Current `.33` sing-box window has no TUIC `fail auth`; if it appears, inspect
  auth/time/config before attributing the run to mini_vpn.
- Reverse-first P1 should leave the `10-20 Mbit/s` class without introducing
  TUN drops, QUIC loss/congestion/blocking, hidden pending, or close/reap loss.
- `tcp-tun-rx-drain` should show bounded TCP ACK/window processing during the
  pressure window if the stage hypothesis is correct.

## Progress Rule

If Knife14ci raises reverse-first P1 materially while keeping terminal pending
and drops clean, overall Knife14 progress can move back toward `85-88%`.
If it stays in the low class with clean drain evidence, the next step should be
an architecture review of local TCP/TUN egress scheduling rather than more
threshold changes.
