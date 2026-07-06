# Knife14cf Proactive Egress Credit Gate Spec

Date: 2026-07-06

## Stage Goal

Move egress credit denial earlier than TUN `tx_dropped` feedback. When local
downlink pressure reaches the credit edge, mini_vpn must clear stale drain
credit and require a bounded amount of observed local drain before extra credit
above clean headroom can be spent again.

## Background

Knife14ce proved that post-drop credit debt is too late for the current VPS
failure. The first positive TUN drop feedback appeared near the close tail:

- `tcp_reverse_window ... send_queue=851955`
- `tcp-tun-egress-feedback ... drop_credit_generation=1`
- subsequent `tcp-downlink-flush` summaries still had all `drop_credit_* = 0`

Therefore, TUN drop feedback is good evidence but not a fast enough control
signal. The earlier signal is local egress pressure itself: `send_queue` and
pending reached the credit edge before sysfs drop sampling reported loss.

## Non-Goals

- Do not change TUIC pool size or reopen pool/stale-slot work.
- Do not solve this by only changing static high/low thresholds.
- Do not tune sing-box, iperf3, VPS services, QUIC congestion, or the egress
  pacer unless new evidence contradicts Knife14cd/ce.
- Do not hide close/reap/pending accounting.

## Invariants

- Clean headroom under `tx_queue_flush_threshold` remains usable.
- Extra drain credit above clean headroom is denied after a local egress
  pressure edge until a bounded debt is repaid.
- A drain observation that repays debt must not grant new credit in the same
  observation; a later clean drain observation is required.
- Drop-triggered and pressure-triggered credit debt are both visible.
- Feedback pause must be able to resume from raw low local pressure; a short
  egress pressure hold must not permanently keep the feedback gate paused.

## Acceptance

Local:

- Focused tests prove:
  - pressure edges install bounded credit debt before a TUN drop;
  - pressure debt clears stale drain credit and blocks extra credit;
  - overpaying debt in a single observed drain does not grant same-observation
    credit;
  - later clean drain grants credit again; and
  - feedback can resume on raw low pressure while the separate pressure hold is
    still active for ordinary downlink backpressure.
- Parser self-tests include pressure-credit fields.
- Full Rust, harness, release, clippy, and whitespace gates pass.

VPS:

- Run the same scoped clean reverse-first P1:
  `.27` client, `.33` sing-box, `.77` target, default pool=2, MTU 1200,
  `PARALLEL_SET=1`, `DURATION=30`.
- Direct `.27 -> .77` and `.33 -> .77` baselines must be healthy.
- Current-window sing-box evidence must be checked for TUIC auth errors.
- Acceptance requires stable high throughput or, if it still fails, a clear
  final summary showing whether proactive pressure credit debt engaged and
  whether TUN drops/pause recovery improved.

## Progress Target

If Knife14cf restores high throughput while reducing TUN drops and preventing a
stuck final feedback pause, Knife14 moves back to `88-90%`. A low-throughput
result with active proactive debt but no improvement is an architecture
re-evaluation point rather than another threshold-tuning prompt.
