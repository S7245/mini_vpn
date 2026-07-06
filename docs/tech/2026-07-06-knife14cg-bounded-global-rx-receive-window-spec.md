# Knife14cg Bounded Global RX Receive Window Spec

Date: 2026-07-06

## Stage Goal

Decouple remote/TUIC receive progress from local TUN egress pressure. Local
tx-queue or TUN-drop pressure may limit how much data is flushed into smoltcp
and the TUN device, but it must not automatically stop `global_rx.recv()` while
there is bounded per-flow receive-window space left.

## Background

Knife14cf proved proactive pressure credit debt is active and useful, but not
sufficient. The scoped VPS run reduced probe TUN drops from Knife14ce's `2813`
to `539` and feedback resumed in the probe, yet reverse-first P1 stayed at
`20.5/19.5 Mbit/s`. The final data stream was live but bursty:

- `send_queue_max=892928`
- `pending_high=582714`
- `final_pending_at_close bytes=574203`
- data stream `max_read_gap_ms=4325`

The current receive gate is too coarse:

```text
global_rx_paused = downlink_rx_paused || tun_egress_feedback.is_paused()
```

That binds relay/TUIC stream reads to local egress spikes. It protects memory,
but it can also feed burst/stall behavior back into the remote TCP sender.

## Non-Goals

- Do not change TUIC pool size, sing-box config, iperf3, stale slot logic, or
  QUIC congestion settings.
- Do not increase memory without a bounded receive window.
- Do not hide close-time pending, terminal pending, or TUN drop accounting.
- Do not solve this by only raising `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH`.

## Invariants

- Local egress pressure still limits `send_slice` and TUN flush behavior through
  existing downlink flush headroom, debt, and hard-edge guards.
- `global_rx.recv()` is paused by a separate bounded receive window based on
  app-owned `downlink_pending`, not by tx-queue-only pressure.
- TUN feedback/drop pauses must remain observable and may continue to install
  credit debt, but they must not be the sole reason remote data stops entering
  bounded pending space.
- The receive window must have hysteresis so it does not flap each time one
  remote payload crosses the high watermark.
- Parser summaries must show receive-window pause/resume separately from local
  egress backpressure.

## Acceptance

Local:

- Tests prove:
  - tx-queue-only pressure can pause local egress backpressure while keeping
    global receive enabled;
  - app-owned pending at the receive-window high watermark pauses global
    receive;
  - the receive window resumes only after pending drains to its low watermark;
  - TUN feedback pause alone does not stop global receive while pending remains
    below the receive-window high watermark; and
  - diagnostic formatting exposes receive-window high/low and local egress
    pause state.
- Parser self-tests include `global_rx_receive` summary and attribution.
- Full Rust, harness, release, clippy, and whitespace gates pass before VPS.

VPS:

- Same scoped clean reverse-first P1 as Knife14cf:
  `.27` client, `.33` exit, `.77` target, default pool=2, MTU 1200,
  `PARALLEL_SET=1`, `DURATION=30`.
- Current-window sing-box evidence must still show no TUIC `fail auth`.
- Acceptance requires reverse-first P1 to leave the 10-20 Mbit/s class with no
  hidden terminal pending/reap loss, or a clear counterexample proving bounded
  receive decoupling did not affect TUIC stream gaps.

## Progress Target

If Knife14cg raises clean reverse-first P1 materially while keeping TUN drops
bounded and preserving close accounting, Knife14 moves to `88-90%`. If it does
not change stream-gap/throughput shape, the remaining issue should be treated
as an architecture confirmation point rather than another threshold/debt patch.

## Post-Run Decision

The scoped VPS run rejected bounded global receive decoupling as a default: it
kept TUIC/global receive moving into bounded pending, but reverse-first P1 stayed
at `20.0/18.7 Mbit/s`, pending grew to `2123091B`, and TUN drops rose to
`4051`. The implementation is therefore gated behind
`MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1`; default behavior keeps the prior
safe `downlink_rx_paused || tun_egress_feedback` receive gate.
