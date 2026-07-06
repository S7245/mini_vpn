# Knife14co Active-Flow ACK Drain Spec

Date: 2026-07-06

## Goal

Repair the remaining Knife14cn reverse-first low-throughput branch by giving an
active reverse TCP flow a small bounded TUN RX ACK/window drain opportunity
while remote payload is being accepted, before local egress reaches the credit
edge.

This is not another static pressure threshold change. Knife14cn already proved
active credit debt can stop pending growth. Knife14co tests whether the sender
window is being allowed to stall before the later pressure-only ACK drain runs.

## Evidence

Knife14cn valid VPS run:

- reverse-first P1 stayed low at `20.5/19.2 Mbit/s`
- pending/close/reap accounting was clean:
  `pending_at_close=0`, `egress_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`
- active debt now paused receive immediately at the credit edge:
  `tcp-egress-credit-debt reason=pressure_credit_edge installed_bytes=24576`
  followed by `tcp-downlink-backpressure paused=true`
- final pending stayed tiny compared with Knife14cm:
  `2035` versus `541802`
- remaining failure signals were local egress drops and unpaid debt:
  `tun_tx_dropped_delta=3524`, `drop_delta_total=2714`,
  `pressure_credit_debt_paid_bytes=0`, `send_queue_max=892928`
- clean surfaces stayed clean: healthy direct baselines, no current `.33`
  `fail auth`, no QUIC loss/congestion/blocking deltas, no send-slice errors,
  and no TUN flush failures

## Design Tree

- External path/server issue: rejected for this branch because direct
  `.27/.33 <-> .77` baselines were healthy and `.33` had no current TUIC auth
  failure evidence.
- Close/reap hiding: rejected for this branch because Knife14cn reported clean
  pending, egress-at-close, terminal reap, and late-payload accounting.
- Receive continuing after pressure debt: fixed by Knife14cn; pending no
  longer grows into the hundreds of KiB after pressure debt is installed.
- Pressure-only ACK drain: insufficient. Earlier stages showed ACK/window
  packets are real and drainable, but pressure-only timing still lets the local
  send queue reach the edge and drop.
- Active-flow ACK drain: next testable branch. While a remote payload is
  actively being accepted, process a small bounded amount of TUN RX so ACKs and
  window updates can free smoltcp send capacity before the pressure edge.

## Non-Goals

- Do not tune stale pool, iperf3, sing-box, egress pacer, QUIC congestion, or
  receive-window sizes from this evidence.
- Do not make TUN RX drain an unconditional timer poll below pressure.
- Do not raise the existing operator override cap.
- Do not move behavior into `scripts/`; scripts only validate the core code.
- Do not hide close/reap accounting or relax terminal-pending diagnostics.

## Invariants

- No downlink work means no TUN RX scan.
- `MINI_VPN_TUN_RX_DRAIN_BUDGET>0` remains an explicit operator override.
- The pressure path keeps the larger ACK-sized pressure budget at or above the
  existing credit edge.
- The new active-flow default budget is smaller than the pressure budget and
  capped independently.
- Maintenance/timer TUN RX drain remains pressure-gated below the credit edge;
  active-flow drain is event-driven by current remote payload work.
- Existing pending, close, debt, and backpressure accounting remains visible.

## Acceptance

Local:

- Add a RED/GREEN test proving active downlink work below the credit edge gets a
  bounded default ACK/window drain budget.
- Prove no-work cases still get zero budget.
- Prove pressure-edge behavior still receives the pressure budget.
- Prove maintenance drain stays off below the credit edge.
- Run focused TUN RX drain tests and the broader regression gates before VPS.

VPS:

- Reverse-first P1 must not be a no-data run.
- Desired improvement: lower TUN drops, paid pressure/drop debt, lower
  `send_queue_max`, smoother receive/read cadence, or throughput materially
  above Knife14cn.
- Clean close/reap accounting must remain clean and explainable.
- `.33` current-window TUIC auth must be checked before attributing any failure
  to mini_vpn data-plane behavior.
