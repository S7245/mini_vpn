# Knife14cl Local Uplink Close Pending Deferral Spec

Date: 2026-07-06

## Goal

Prevent `uplink_channel_closed` from rearming a socket while useful reverse
downlink bytes remain in `downlink_pending` or in the active send-capable local
egress queue.

Knife14ck showed the actionable close-tail state:

- `direction=local_to_remote reason=uplink_channel_closed`
- `pending=524906`
- `close_pending_class=active_send_capable`
- `close_egress_bytes=892928`
- `close_egress_drain_candidate=true`
- `tcp_state=CloseWait can_send=true may_send=true`

The local close path must treat this the same way as a relay close with pending
downlink: defer rearm, keep the handle dirty, flush pending bytes first, then
hold the close while active egress queue remains above the low watermark.

## Non-Goals

- Do not change TUIC, sing-box, stale pool, iperf3, receive-window expansion,
  or static ACK-drain budget behavior.
- Do not make pending or egress queues unbounded.
- Do not hide terminal pending; terminal no-send/closed accounting remains
  visible.
- Do not alter script thresholds as the fix.

## Invariants

- If `ctx.downlink_pending` is non-empty and the close epoch matches, rearm is
  deferred regardless of whether the close event arrived from remote relay close
  or local uplink-channel closure.
- Pending close deferral records the close reason, enters `Closing`, clears the
  uplink sender, records pending progress timestamps, and leaves the socket
  generation unchanged.
- After pending drains, the existing egress close-drain guard continues to hold
  active send-capable queues above `low_bytes` for a bounded grace window.
- If no pending exists, the existing egress-only close deferral behavior remains
  unchanged.
- Empty-pending local close may still rearm immediately when it is not an
  egress drain candidate.

## Acceptance

Local:

- Focused tests prove pending close deferral is direction-agnostic and records
  progress state.
- Existing close/reap, TUN RX drain, pressure-credit, and full regression gates
  continue to pass.

VPS:

- Reverse-first P1 should not show an immediate `tcp-handle-close` for
  `uplink_channel_closed` with active send-capable pending bytes.
- If pending remains, logs should show deferred close state rather than rearm.
- Target signal is reverse-first P1 leaving the 10-20 Mbit/s band. If throughput
  still stays low, the run must at least prove whether pending/egress drains
  after local close or whether another scheduling point is still starving.
