# 2026-07-08 Knife14gq Buffered Downlink Results

## Goal

Execute the B0-B7 buffered-downlink architecture slice and stop at B7.

Acceptance for this slice was deliberately narrow:

- enable the feature-flagged buffered downlink controller;
- preserve bounded hard pressure guards;
- prove the focused safe1200 reverse-first P1 exceeds `30 Mbit/s`;
- stop and reassess if it does not.

Non-goals stayed unchanged: no VPS retune, no iperf3 changes, no MTU/PLPMTUD
work, no stale-pool work, no broad QUIC window work, and no continuation to
B8/B9 after a failed B7.

## Code

- Commit: `8a85ce7` (`fix(knife14gq): add buffered downlink controller`)
- Spec:
  `docs/tech/2026-07-08-knife14gq-buffered-downlink-architecture-spec.md`
- Main change:
  `SocketCtx.downlink_pending` is now an explicit feature-flagged buffered
  downlink layer for remote TUIC read-credit decisions.
- Feature flag:
  `MINI_VPN_BUFFERED_DOWNLINK=1`
- Watermarks in the B7 run:
  - per-flow low/high/hard: `327272/1309088/2618176B`
  - global low/high/hard: `654544/2618176/5236352B`

This was a framework change, not a parameter tune: buffered mode decouples TUIC
stream read service from transient smoltcp send-queue/headroom pressure, while
retaining terminal no-recv, per-flow hard, global hard, and explicit hard
TUN/drop feedback pauses.

## TDD And Gates

Local gates passed:

- `rustfmt --edition 2024 src/client_tun.rs`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `git diff --check`
- `cargo test buffered_downlink -- --nocapture`
- `cargo test relay_read_credit_publish_wakes_on_egress_progress_even_when_credit_is_unchanged -- --nocapture`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo build --release`

New/updated focused tests included:

- buffered watermark derivation from existing downlink backpressure;
- transient local headroom pressure does not shrink buffered read credit;
- soft high keeps a useful `65536B` read floor instead of collapsing to MTU;
- per-flow and global hard watermarks pause and resume with hysteresis;
- buffered global receive backpressure uses pending-buffer watermarks;
- runtime config disables buffered mode by default.

Remote `.27` workdir:

- `/tmp/mini_vpn_knife14gq_8a85ce7`

Remote `.27` gates passed:

- `cargo test buffered_downlink -- --nocapture`
- `cargo test relay_read_credit_publish_wakes_on_egress_progress_even_when_credit_is_unchanged -- --nocapture`
- `cargo build --release`

## Acceptance Setup

Preflight:

- `.33` sing-box: `active`
- `.77` iperf3: `active`
- suite direct `.27 -> .77` forward receiver: `297 Mbit/s`
- suite direct `.27 <- .77` reverse receiver: `278 Mbit/s`

Tunnel run:

- safe1200 reverse-first P1
- `DURATION=30`
- `PARALLEL_SET=1`
- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `MINI_VPN_BUFFERED_DOWNLINK=1`
- `IPERF_TIMEOUT_SECS=120`

Bundle:

- Remote:
  `/tmp/knife14gq_b7_8a85ce7/mvpn_knife14gq_buffered_downlink_safe1200_p1_30_usclient_suite_20260708_235459.tar.gz`
- Local:
  `/tmp/mini_vpn/knife14gq_buffered_downlink_20260708/mvpn_knife14gq_buffered_downlink_safe1200_p1_30_usclient_suite_20260708_235459.tar.gz`

## VPS Result

Focused safe1200 reverse-first P1:

- Sender: `18.3 Mbit/s`
- Receiver: `17.2 Mbit/s`
- Stage answer: **No, B7 did not exceed `30 Mbit/s`.**

The interval profile stayed burst/idle. The first second reached
`81.8 Mbit/s`, later burst seconds reached `52.4`, `70.3`, and
`49.3 Mbit/s`, but many intervals were `0.00`, leaving the receiver average at
`17.2 Mbit/s`.

## Key Signals

Positive:

- Buffered mode was enabled at startup.
- The old Knife14gp read-service collapse was fixed in this run:
  - `remote_read_service_len_min=65536`
  - `remote_read_service_len_max=65536`
  - `remote_batch_limit_bytes_min=524288`
  - `read_credit_pause_updates=0`
  - `read_credit_limit_bytes_min=524288`
- The data stream moved `64510664B`.
- Global receive pressure stayed clear:
  `global_rx_pressure events=0`, queue max `53/1024`,
  `global_rx_receive pause_edges=0 resume_edges=0`.
- Low-level send errors stayed clear:
  `send_slice_zero=0`, `send_slice_errors=0`.
- TUN drops stayed clear:
  `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=0`.
- QUIC loss/congestion/blocking stayed clear:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_data_delta=0`, `max_tx_blocked_stream_delta=0`,
  `max_rx_blocked_data_delta=0`, `max_rx_blocked_stream_delta=0`.
- Close-tail accounting stayed clean:
  `pending_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`, `egress_at_close=0`.

Still failing:

- Throughput shape stayed low average:
  `shape=low_average tail_collapse=0 local_pressure=1 no_data=0 stable_high=0`.
- Attribution stayed local:
  `attribution: local_downlink_backpressure`.
- Local downlink pressure still touched the edge:
  `downlink_backpressure pause_edges=1 resume_edges=1`,
  `max_tx_queue_bytes=557386`, `max_total_pressure_bytes=898944`.
- The local writer was clean but still cadence-limited:
  `send_slice_accepted=64510664`, `budget_limited=10`,
  `headroom_limited=11`, `headroom_deferred_bytes=603544`,
  `tun_flush_deferred=4`.
- Ordered delivery still had multi-second gaps:
  `data_read_gap_max_ms=3581`,
  `data_pending_gap_max_ms=3007`,
  `tuic_stream_pending events=18`.
- Pending causes were still mostly ordered-stream delivery gaps:
  `connection_stream_frames_pending=15`,
  `connection_rx_no_stream_frames=3`.

## Interpretation

The buffered architecture direction did one important thing correctly: it
stopped the TUIC stream reader from collapsing to `1200B` service under
transient local headroom pressure. That is real framework progress.

It did **not** validate the throughput path. The B7 acceptance failed below
`30 Mbit/s`, and therefore this slice cannot be used to claim a clear
`20M -> 100M+` implementation path.

The bottleneck moved rather than disappeared:

- before B7, Knife14gp's most actionable symptom was read-credit collapse to
  `1200B`;
- after B7, read service stayed useful (`65536B` service, `524288B` credit),
  but the local downlink writer/egress cadence still produced burst/idle
  delivery and local pressure attribution.

The next architecture decision should be made from this evidence, not by
continuing small pressure-credit tweaks. A future B8 would need to target the
local writer/egress scheduling path directly and prove, with tests first, that
local TUN/smoltcp egress can sustain continuous delivery instead of second-scale
bursts separated by ordered-stream pending gaps.

## Stop Point

Per the stage rule, work stopped at B7 because the focused gate did not exceed
`30 Mbit/s`.

No B8/B9 work was started.
