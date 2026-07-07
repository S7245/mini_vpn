# Knife14cs Broader TCP Regression Spec

Date: 2026-07-07

## Goal

Close the remaining Knife14 TCP acceptance gap by proving the Knife14cq/cr
default path is not only a clean reverse-first P1 recovery. The same source and
binary must survive the broader TCP tunnel sweep on the `.27 -> .33 -> .77`
path while keeping close/downlink lifecycle accounting explicit and clean.

## Evidence

Knife14cq restored reverse-first P1 to `182/181 Mbit/s` and Knife14cr repeated
the same path at `174/172 Mbit/s`. Both runs used the same default-disabled
recent-active timer drain path:

- `MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS` unset, startup timer drain `0ms`
- `timer_active_flow_attempts=0`
- no terminal pending reap or terminal late remote payload
- no clean-window TUN drops, send-slice errors, TUN flush failures, or QUIC
  loss/congestion/blocking
- no current `.33` TUIC `fail auth`

## Non-Goals

- Do not change code unless the broader sweep produces a new, parsed failure
  signal.
- Do not retune stale pool, iperf3, sing-box, egress pacer, or TUN qdisc paths
  without fresh evidence.
- Do not treat test convenience scripts as product behavior; they are used only
  to run and bundle acceptance evidence.
- Do not hide terminal pending/close/reap data behind aggregate throughput.

## Acceptance

- Run the broader suite with reverse-first P1 enabled and full sweep enabled:
  `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=0`,
  `PARALLEL_SET="1 2 4 8"`, MTU `1200`, `DURATION=30`.
- The reverse-first P1 probe remains outside the rejected `10-20 Mbit/s` band
  and preferably remains `stable_high`.
- Standard P1 and full sweep results show no lifecycle loss class:
  `pending_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`, no send-slice/TUN flush errors, and no
  clean-window QUIC loss/congestion/blocking.
- Any active send-capable close-egress tail remains visible and is interpreted
  only with the probe throughput shape and terminal state.
- `.33` service and current-window auth checks stay clean.
- `.27` cleanup succeeds: no active `client-tun`, target route restored, and
  no stale `tun0` address.

## Failure Handling

If the broader sweep fails, parse the bundle before editing code. The next
patch must name the specific failing class, such as higher-concurrency
backpressure, a quiet-wait leak, terminal pending, auth/config failure, or a
new local lifecycle signal.
