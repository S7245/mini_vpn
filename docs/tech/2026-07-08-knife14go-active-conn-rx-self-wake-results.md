# 2026-07-08 Knife14go Active Connection-RX Self-Wake Results

## Goal

Continue after Knife14gn without changing VPS config, iperf3, MTU/PLPMTUD,
stale pool handling, broad QUIC windows, or local pressure-credit constants.

The focused question was whether TUIC ordered-stream receive cadence improves
when pending reads self-wake not only for `connection_stream_frames_pending`,
but also while the QUIC connection is actively receiving packets before stream
frames become deliverable.

The acceptance target for this slice was the first recovery step:
`>30 Mbit/s` on the same focused safe1200 reverse-first P1.

## Code

- Commit: `39112ca` (`fix(knife14go): self-wake active tuic receive`)
- Change: `should_arm_tuic_stream_pending_self_wake` now arms bounded self-wake
  for active connection RX causes:
  - `connection_stream_frames_pending`
  - `connection_rx_no_stream_frames`
- The change does not cancel/recreate an in-flight relay read and does not
  change read-credit, pressure-credit, MTU, pool, or QUIC window constants.

## TDD

Red test added first:

- `cargo test tuic_stream_pending_self_wake_services_active_connection_rx -- --nocapture`

The test initially failed because `ConnectionRxNoStreamFrames` did not arm
self-wake. After the code change, the focused self-wake tests passed.

Local gates:

- `rustfmt --edition 2024 --check src/tuic.rs`
- `cargo test tuic_stream_pending_self_wake -- --nocapture`
- `cargo test tuic_tcp_stream_pending_cause_classifies_transport_progress_since_read -- --nocapture`
- `cargo test relay_read_credit_update_keeps_inflight_remote_read_armed -- --nocapture`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo build --release`

Remote `.27` workdir:

- `/tmp/mini_vpn_knife14go_39112ca`

Remote `.27` gates:

- `cargo test tuic_stream_pending_self_wake -- --nocapture`
- `cargo build --release`

## Acceptance Setup

Preflight:

- `.33` sing-box: `active`
- `.77` iperf3: `active`
- suite direct `.27 -> .77` forward receiver: `285 Mbit/s`
- suite direct `.27 <- .77` reverse receiver: `281 Mbit/s`
- tunnel mode: safe1200 reverse-first P1, `DURATION=30`, `PARALLEL_SET=1`,
  `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
  `IPERF_TIMEOUT_SECS=120`

Bundle:

- Remote:
  `/tmp/conn/mvpn_knife14go_active_connrx_selfwake_safe1200_p1_30_usclient_suite_20260708_213110.tar.gz`
- Local:
  `/tmp/mini_vpn/knife14go_active_connrx_selfwake_20260708/mvpn_knife14go_active_connrx_selfwake_safe1200_p1_30_usclient_suite_20260708_213110.tar.gz`

## VPS Result

Focused safe1200 reverse-first P1:

- Sender: `24.6 Mbit/s`
- Receiver: `22.4 Mbit/s`
- Stage answer: **No, Knife14go did not exceed `30 Mbit/s`.**

The run did restore a slightly better data-moving shape than Knife14gn
(`18.0 Mbit/s` receiver), but it stayed low-average and burst/idle.

## Key Signals

Positive:

- The suite completed normally: `status: COMPLETED`, `exit_code: 0`.
- Close-tail accounting was clean:
  `pending_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`, `egress_at_close=0`.
- TUN drops stayed clear:
  `tun_tx_dropped_delta=0`.
- QUIC loss/congestion/blocking stayed clear:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_data_delta=0`, `max_tx_blocked_stream_delta=0`,
  `max_rx_blocked_data_delta=0`, `max_rx_blocked_stream_delta=0`.
- Data stream polling cadence improved materially:
  `data_poll_gap_max_ms=43`, while Knife14gn had a multi-second poll gap.
- The stream was actively self-waking:
  `self_wake_armed=11702`, `self_wake_fired=8726`.
- The data stream moved `84087254` bytes, so this was not the earlier no-data
  failure.

Still failing:

- Throughput shape remained low-average:
  `throughput_shape=shape=low_average tail_collapse=0 local_pressure=1 no_data=0 stable_high=0`.
- Data stream delivery still had multi-second gaps despite frequent polling:
  `data_read_gap_max_ms=3872`, `data_pending_gap_max_ms=3006`.
- Pending cause remained connection-level stream frame progress:
  `connection_stream_frames_pending=20`,
  `connection_rx_no_stream_frames=0`.
- The final window reintroduced a local pressure edge:
  `downlink_backpressure pause_edges=1 resume_edges=1`,
  `read_credit_pause_updates=1`,
  `pressure_credit_debt_bytes=122727`,
  `pressure_credit_blocked_bytes=122727`.
- The local pressure edge appears after burst catch-up, not as the initial
  root: earlier snapshots had clean pressure while ordered-stream pending gaps
  were already present.

## Interpretation

Knife14go was a useful discriminator, but not the fix.

The self-wake lane is now servicing pending reads frequently: the data stream
poll gap dropped to `43ms`. However, application reads still experience
`~3.9s` ordered-stream delivery gaps while connection-level QUIC stats show
stream frames arriving and no QUIC loss/blocking.

That means the next bug is not "the relay reader is asleep for seconds." It is
closer to "the connection receives stream frames, but the ordered stream is not
continuously deliverable to mini_vpn." The late local pressure/credit edge then
appears during catch-up bursts and still needs to be handled, but tuning those
constants first would not explain the multi-second ordered delivery gaps.

## Next

Do not continue by changing pressure-credit constants blindly.

The next code slice should add a per-stream deliverability discriminator:

1. Separate connection-level STREAM frame progress from current-stream
   deliverable progress in diagnostics/tests.
2. Record whether the pending stream is being frequently polled but still
   returns `Pending` while the connection receives frames.
3. Compare mini_vpn's ordered read service against the sing-box client model:
   read loop cadence, stream buffering, and write-backpressure coupling.
4. Only after the deliverability branch is understood, address the late
   local-pressure edge that appears during catch-up bursts.

The next acceptance target remains `>30 Mbit/s` on focused safe1200
reverse-first P1 before resuming the `100+ Mbit/s` finish path.
