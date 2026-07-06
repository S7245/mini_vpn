# Knife14cr Repeat Stability Spec

Date: 2026-07-06

## Goal

Confirm Knife14cq was not a one-off high result by repeating the same clean
reverse-first P1 VPS suite with the default-disabled recent-active timer path.

## Evidence

Knife14cq passed one scoped VPS run:

- reverse-first P1: `182/181 Mbit/s`
- `throughput_shape=stable_high`
- `timer_active_flow_attempts=0`
- `pending_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`
- no TUN drops, QUIC loss/congestion/blocking, send-slice errors, or TUN flush
  failures

## Non-Goals

- Do not change code in this stage.
- Do not tune egress pacer, stale pool, sing-box, iperf3, receive window, or
  TUN qdisc length.
- Do not treat an active send-capable close-egress tail as terminal pending
  loss unless it correlates with throughput failure or terminal no-send state.

## Acceptance

- Repeat reverse-first P1 reaches stable high throughput or at least remains
  materially outside the 10-20 Mbit/s tier.
- `timer_active_flow_attempts=0`.
- Pending/terminal/reap accounting remains clean.
- No current `.33` TUIC `fail auth`.
- If the repeat fails, analyze the bundle before code changes.
