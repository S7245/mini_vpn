# Knife14bf spec - TUN-drop-aware downlink feedback

Date: 2026-07-05

## Grounding

Knife14be tested the receive-window hypothesis by raising the local TCP tx
buffer to 4MiB while leaving downlink backpressure on `<auto>`.

The clean reverse-first P1 result was worse than the 1MiB run:

- 4MiB receiver: `18.9 Mbit/s`
- 1MiB receiver: `26.2 Mbit/s`
- 4MiB backpressure: `pause_edges=0`
- 4MiB `send_queue_max=3786786`, below the 4MiB high watermark
- 4MiB clean-window TUN drops: `tun_tx_dropped_delta=27530`
- runtime TUN egress drops: `drop_delta_total=14946`, `max_delta=14167`
- clean-window QUIC loss/congestion: `0`
- terminal pending/reap during the clean window: `0`

This falsifies the simple "larger local receive window fixes reverse
throughput" branch. A larger smoltcp tx buffer hid local pressure from
watermark backpressure until the Linux TUN/qdisc path dropped packets.

Knife14at already proved that TUN drops can coexist with high reverse
throughput, so this slice must not treat any drop as a complete root cause. The
new, narrower problem is: a large tx buffer must not let TUN drops occur while
global remote reads remain fully unpaused and unattributed.

## Goal

Add a bounded, observable feedback loop for Linux TUN egress drops:

- keep normal high/low downlink backpressure as its own state;
- keep runtime TUN egress drop feedback as a separate pause reason;
- sample TUN egress drops frequently enough to affect the next clean-window run;
- when a positive TUN drop delta is observed while local downlink pressure
  exists, pause `global_rx`;
- resume that TUN feedback pause when local pressure drains to the configured
  low watermark;
- log parser-visible counters so reports can distinguish ordinary
  high-watermark pause from TUN-drop-triggered pause.

## Non-goals

- Do not increase TCP tx buffers further.
- Do not tune TUN queue length.
- Do not change sing-box, iperf3, stale pool behavior, or QUIC congestion
  settings.
- Do not re-enable the rejected fixed 64KiB immediate egress pacer as a
  default.
- Do not claim TUN drops alone explain all low-throughput windows.
- Do not store secrets or raw VPS environment output in docs or learnings.

## Invariants

- `downlink_pending` still owns bytes not accepted by smoltcp.
- Existing backpressure high/low hysteresis remains intact.
- TUN feedback pause is additive: `global_rx` is paused when either ordinary
  downlink backpressure or TUN egress feedback is active.
- TUN feedback cannot permanently wedge the relay: it resumes once local
  pressure is at or below the low watermark and no new drop delta is observed.
- Missing interface name, non-Linux sysfs, read errors, parse errors, and
  counter resets do not pause data flow.
- Parser output keeps terminal pending, downlink backpressure, runtime TUN
  drops, and TUN feedback as separate report lines.

## Acceptance

Local:

- unit tests cover TUN feedback pause/resume/no-op decisions;
- low-RTT parser self-test covers a clean-window shape where tx queue is below
  high watermark but TUN feedback pauses;
- `cargo test --lib client_tun::tests::tun_egress_feedback`;
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`;
- suite script self-test still passes;
- `git diff --check`.

VPS after local gates:

- run one scoped reverse-first suite from `.27`;
- startup path and direct baselines must be healthy;
- clean P1 must report `tun_egress_feedback` separately from
  `downlink_backpressure`;
- if throughput remains low, the bundle must show whether the active limiter is
  high-watermark pause, TUN feedback pause, terminal pending/reap, QUIC
  congestion/loss, or no visible local pressure.
