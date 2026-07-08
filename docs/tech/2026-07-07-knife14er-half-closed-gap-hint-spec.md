# Knife14er Half-Closed Gap Hint Spec

Date: 2026-07-07

## Stage Goal

Close the Knife14eq no-pressure reverse stall branch by keeping relay read-gap
ACK/window maintenance alive after the local writer half has finished.

Knife14eq showed a very different failure from the local pressure/debt spiral:
reverse-first P1 failed at `0.699/0.030 Mbit/s`, while local pressure stayed at
zero and read credit never collapsed below `65536` bytes. The new signal was
late remote data after local finish plus long TUIC stream pending/read gaps.

## Evidence

Knife14eq scoped safe1200 reverse-first P1:

- `throughput_shape: shape=no_data ... local_pressure=0 no_data=1`
- `downlink_backpressure pause/resume=0/0`
- `headroom_deferred_bytes=0`
- `pending_max=0`
- `read_credit_limit_bytes_min=65536`
- `tun_tx_dropped_delta=0`
- QUIC loss/congestion/blocking all zero.
- `relay_late_remote post_finish_bytes=92288 post_finish_reads=4`
- `tuic_stream_pending data_pending_gap_max_ms=19922`
- `tuic_tcp_stream data_read_gap_max_ms=20491`

The current relay gap-hint gate rejects all hints once the local writer is done,
even though the read half can still legally receive reverse payload until remote
EOF.

## Non-goals

- Do not tune stale pool, sing-box, iperf3, QUIC MTU/PLPMTUD, TUN queue length,
  or the rejected timer-driven recent-active ACK drain.
- Do not enlarge local pending or disable bounded staging.
- Do not hard-pause QUIC receive in the no-pressure branch.
- Do not treat this as pressure-controller acceptance until VPS proves
  throughput recovery.

## Required Behavior

- Data-bearing relay streams may emit read-gap ACK/window hints after local
  writer shutdown.
- Pure control streams and relays with no remote data still do not emit gap
  hints.
- Hints remain rate-limited by the existing gap and interval thresholds.
- Main-loop budget selection continues to require current relay epoch, relay or
  closing state, and local egress headroom.
- Close/reap accounting remains bounded and explicit.

## Acceptance

Local:

- Focused relay gap-hint test covers half-closed data streams.
- Existing relay local-finish, relay-read, deferred ACK, relay-gap, downlink
  controller, and projected pressure tests remain green.
- Full lib/harness/clippy/script/release gates pass before final acceptance.

VPS:

- `.27 -> .33 -> .77`, safe1200, reverse-first P1.
- Receiver `100+ Mbit/s`.
- `tun_tx_dropped_delta=0`.
- `pending_at_close=0`, `terminal_pending_reap=0`, `egress_at_close=0`.
- QUIC loss/congestion/blocking remain zero.
- If failure remains, attribution must distinguish no-pressure read gaps from
  pressure/debt paths before any further code change.
