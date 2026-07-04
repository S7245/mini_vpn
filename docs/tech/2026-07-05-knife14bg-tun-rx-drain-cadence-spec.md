# Knife14bg TUN RX Drain Cadence Spec

Date: 2026-07-05

## Stage Goal

Reduce clean reverse-first TCP P=1 downlink burst/idle behavior by giving local
TUN ingress packets, especially TCP ACK/window-update packets, bounded
opportunistic drain opportunities while remote downlink data is flowing.

The stage is successful only if clean reverse-first P1 materially improves and
ordinary `downlink_backpressure` plus terminal pending/reap shrink. Reducing
TUN drop counters alone is not sufficient.

## Grounding

Knife14bf on `e661613` proved:

- clean reverse-first remained `22.8/21.6 Mbit/s`;
- direct `.27 -> .77` and `.33 -> .77` baselines were healthy;
- clean-window QUIC loss/congestion was zero;
- `downlink_backpressure` still reached the 1MiB smoltcp tx queue watermark;
- close/reap still showed `terminal_pending_reap=1108466B`;
- TUN sysfs drop feedback can fire on VPS, but did not fire in clean P1 because
  the drop delta was sampled after current downlink pressure was already gone.

Current event-loop shape:

- remote downlink bytes enter through the `global_rx.recv()` branch;
- local TUN ingress enters only through the `device.wait_for_rx()` branch, which
  reads one packet and then runs `iface.poll`;
- the 5ms timer runs `iface.poll`, but does not read new packets from TUN;
- therefore local TCP ACK/window-update packets can wait behind a constantly
  ready remote downlink branch until ordinary backpressure pauses `global_rx`.

## Design Tree

Branch A: remote downlink fairness starves TUN ingress.

- Expected signal: smoltcp tx queue hits high watermark even though TUN writes
  themselves do not fail.
- Mechanism: remote payloads are accepted into smoltcp faster than local ACKs
  are read from TUN and applied.
- Fix shape: after remote payload handling, opportunistically drain a bounded
  number of already-ready TUN ingress packets without blocking.

Branch B: smoltcp/TUN egress writes are the direct bottleneck.

- Expected signal: `flush_tx` failures, sustained TUN drop deltas while pressure
  is current, or loop profiler showing poll/flush saturation.
- Current evidence is weaker: clean P1 had `tun_flush_failures=0` and the new
  feedback gate did not observe current pressure with the drop sample.

Branch C: close/reap loses data independently of throughput.

- Expected signal: terminal pending appears without prior ordinary backpressure
  or tx-queue pressure.
- Current evidence is weaker: terminal pending follows repeated 1MiB tx queue
  pressure, so it is more likely the tail of an earlier drain-cadence failure.

Branch D: receive-window capacity is too small.

- Rejected for now. Knife14be 4MiB made clean reverse worse and moved pressure
  into TUN/qdisc drops.

## Proposed Behavior

Add a small non-blocking TUN RX drain path:

- Extend the `TunIo` seam with a method that attempts to read one already-ready
  TUN packet into the existing `rx_buffer` without awaiting.
- For production Linux/macOS paths, use the underlying nonblocking TUN fd via
  `tun::AsyncDevice::get_mut().read(...)`, treating `WouldBlock` as no packet.
- For the harness loopback device, pop one queued inbound packet if available.
- Add a bounded `drain_ready_tun_rx` helper in `client_tun.rs` that processes up
  to `N` ready TUN packets and runs the same classification/poll/dirty logic as
  the existing awaited `wait_for_rx` branch.
- Call this helper after remote downlink payload handling when bytes were
  accepted or pending remains, and from timer/egress paths if pressure remains.

Default budget should be conservative, for example 8 packets per drain pass, and
configurable only if local evidence requires it.

## Invariants

- Never block inside opportunistic drain.
- Never process UDP/DNS/TCP packets through a different semantic path than the
  awaited TUN RX branch.
- Preserve dirty-handle lifecycle and fake-IP refcount rules.
- Preserve bounded backpressure: remote downlink must still pause when local
  pressure reaches the high watermark.
- A single flow must not monopolize the loop; the drain budget is a hard cap.
- No secrets or raw env dumps in logs, docs, or learning memory.

## Non-Goals

- No stale pool changes.
- No sing-box, iperf3, or VPS provider tuning.
- No larger TCP tx buffer / receive-window tuning.
- No TUN queue length tuning.
- No egress pacer default tuning unless the new drain evidence contradicts the
  current branch.

## Acceptance

Local acceptance:

- unit tests for nonblocking drain semantics and bounded budget behavior;
- existing `cargo test --lib client_tun`;
- `cargo test --lib device` if tests are added there;
- parser self-test if a new diagnostic line is parsed;
- `git diff --check`.

VPS acceptance:

- run `.27` clean reverse-first P1 with `MINI_VPN_TCP_DIAG=1`;
- direct baselines must still be healthy;
- clean reverse-first receiver throughput must improve materially from
  `21.6 Mbit/s`;
- `downlink_backpressure` pause edges and max terminal pending/reap should fall;
- no increase in `tun_flush_failures`, `send_slice_zero`, or `send_slice_errors`;
- if TUN drops remain, attribution must show whether they occur with current
  pressure or only as delayed sysfs samples.
