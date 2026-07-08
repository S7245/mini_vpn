# 2026-07-08 Knife14gp Post-Flush Pressure Debt Results

## Goal

Continue from Knife14go through the next focused T13/T14 slice without changing
VPS configuration, iperf3, MTU/PLPMTUD, stale pool handling, or broad QUIC
window settings.

The stage had two questions:

1. Add diagnostics that separate connection-level STREAM progress since the
   last stream read from progress since the previous pending sample.
2. Move projected downlink pressure debt from the pre-flush transient pending
   state to the post-flush residual pressure state, then check whether the
   focused safe1200 reverse-first P1 clears `30 Mbit/s`.

The acceptance target for this slice was still the first recovery step:
`>30 Mbit/s` on the same focused safe1200 reverse-first P1.

## Code

- Commit: `bd0d264` (`fix(knife14gp): defer projected pressure debt post-flush`)
- `src/tuic.rs`:
  - added since-last-pending transport counters to
    `tuic-tcp-stream-pending`;
  - pending logs now include
    `conn_udp_rx_since_pending={}/{}B` and
    `conn_rx_stream_frames_since_pending={}`.
- `src/client_tun.rs`:
  - remote payload handling now flushes accepted bytes before installing
    projected pressure debt;
  - fully accepted transient payload with no residual pending no longer
    creates pressure debt;
  - residual pending pressure still installs bounded debt through the new
    post-flush helper.

This was a framework/feedback-path change, not a constant tune: the controller
now distinguishes transient pre-flush payload from residual post-flush local
pressure.

## TDD And Gates

Focused tests added or updated:

- `format_tuic_tcp_stream_pending_line_includes_transport_delivery_counters`
- `tuic_tcp_stream_pending_event_reports_since_last_pending_delta`
- `post_flush_projected_pressure_debt_ignores_fully_accepted_transient_payload`
- `post_flush_projected_pressure_debt_installs_for_residual_pending_pressure`

Local gates passed:

- `rustfmt --edition 2024 --check src/tuic.rs src/client_tun.rs`
- `git diff --check`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo build --release`
- focused downlink/TUIC tests for stream-pending, post-flush debt,
  read-credit updates, credit controller, and pressure-credit debt.

Remote `.27` workdir:

- `/tmp/mini_vpn_knife14gp_bd0d264`

Remote `.27` gates passed:

- `cargo test tuic_tcp_stream_pending -- --nocapture`
- `cargo test post_flush_projected_pressure_debt -- --nocapture`
- `cargo test relay_read_credit_update_keeps_inflight_remote_read_armed -- --nocapture`
- `cargo build --release`

## Acceptance Setup

Preflight:

- `.33` sing-box: `active`
- `.77` iperf3: `active`
- suite direct `.27 -> .77` forward receiver: `277 Mbit/s`
- suite direct `.27 <- .77` reverse receiver: `281 Mbit/s`
- tunnel mode: safe1200 reverse-first P1, `DURATION=30`, `PARALLEL_SET=1`,
  `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
  `IPERF_TIMEOUT_SECS=120`

Bundle:

- Remote:
  `/tmp/conn/mvpn_knife14gp_postflush_debt_safe1200_p1_30_usclient_suite_20260708_222938.tar.gz`
- Local:
  `/tmp/mini_vpn/knife14gp_postflush_debt_20260708/mvpn_knife14gp_postflush_debt_safe1200_p1_30_usclient_suite_20260708_222938.tar.gz`

## VPS Result

Focused safe1200 reverse-first P1:

- Sender: `15.0 Mbit/s`
- Receiver: `13.5 Mbit/s`
- Stage answer: **No, Knife14gp did not exceed `30 Mbit/s`.**

This regressed from Knife14go (`24.6/22.4 Mbit/s`) even though the direct
reverse baseline remained healthy.

The iperf intervals stayed burst/idle: early and mid-run bursts reached
individual intervals such as `66.0`, `90.2`, and `59.8 Mbit/s`, but many
intervals reported `0.00`, leaving the average at `13.5 Mbit/s`.

## Key Signals

Positive:

- The suite completed normally.
- Close-tail accounting was clean:
  `pending_at_close=0`, `terminal_pending_reap=0`, `egress_at_close=0`.
- TUN drops stayed clear:
  `tun_tx_dropped_delta=0`.
- QUIC loss/congestion/blocking stayed clear:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_data_delta=0`, `max_tx_blocked_stream_delta=0`,
  `max_rx_blocked_data_delta=0`, `max_rx_blocked_stream_delta=0`.
- Low-level send errors stayed clear:
  `send_slice_zero=0`, `send_slice_errors=0`.
- Global receive pressure stayed clear:
  `global_rx_pressure=0`, `global_rx_receive pause_edges=0`.
- Downlink backpressure pause/resume edges dropped to zero:
  `downlink_backpressure pause_edges=0 resume_edges=0`.

Still failing:

- Throughput shape remained low-average:
  `shape=low_average tail_collapse=0 local_pressure=1 no_data=0 stable_high=0`.
- Attribution stayed on the local pressure-credit branch:
  `local_pressure_credit`.
- Read credit still collapsed to a minimum service size:
  `remote_read_service_len_min=1200`,
  `remote_batch_limit_bytes_min=1200`,
  `read_credit_pause_updates=0`.
- Residual local-pressure guards still fired:
  `may_recv_false=13`, `headroom_limited=3`,
  `headroom_deferred_bytes=91906`,
  `hard_edge_guard_limited=3`,
  `hard_edge_guard_deferred_bytes=35278`,
  `tun_flush_deferred=8`.
- Pressure debt was no longer newly installed as pre-flush debt, but the path
  still blocked and repaid existing/projected pressure:
  `pressure_credit_debt_bytes=0`,
  `pressure_credit_blocked_bytes=299008`,
  `pressure_credit_debt_paid_bytes=299008`.
- Ordered delivery still had multi-second gaps:
  `data_max_read_gap_ms=4069`,
  `data_pending_gap_max_ms=4006`.

## New Diagnostic Value

The T13 since-last-pending counters were useful.

Earlier logs could make `connection_stream_frames_pending` look like continuous
new data because they counted stream-frame progress since the last successful
stream read. Knife14gp showed many repeated pending windows where the same
stream-read gap still had no new connection-level stream frames since the
previous pending sample:

- `pending_gap_ms=2004 ... conn_rx_stream_frames_since_pending=0`
- `pending_gap_ms=3005 ... conn_rx_stream_frames_since_pending=0`
- `pending_gap_ms=4006 ... conn_rx_stream_frames_since_pending=0`
- later repeated gaps with
  `conn_rx_stream_frames_since_pending=0` under both
  `connection_stream_frames_pending` and `connection_rx_no_stream_frames`
  causes.

This means frequent polling/self-wake can be working while there is no fresh
deliverable ordered-stream progress during the gap. The older
`conn_rx_stream_frames_since_read` counter alone was too stale for this
decision.

## Interpretation

Knife14gp fixed one feedback-path shape but did not unlock throughput.

Moving projected pressure debt to post-flush residual pressure removed the
obvious false `downlink_backpressure` pause/resume edges in this run. That is
real architectural progress. However, the average throughput fell to
`13.5 Mbit/s`, and the remaining signal is more specific:

- the data path is not blocked by VPS capacity, iperf3, MTU, QUIC loss, QUIC
  flow-control blocking, TUN drops, or close-tail reaping;
- the relay is not simply asleep for seconds;
- the local controller can still compress useful read service down to `1200`
  bytes under residual pressure/headroom evidence even when send-side errors
  and TUN drops are zero;
- repeated pending logs often show stale connection progress rather than fresh
  stream-frame delivery since the previous pending sample.

So the next slice should not be another self-wake timer or VPS retune. It
should use the new since-last-pending discriminator and prevent useful
read-credit collapse when accepted egress is progressing and there is no hard
drop/failure signal.

## Next

Stop after this stage.

The next code slice should be TDD-first around:

1. classifying stale repeated STREAM-pending samples separately from fresh
   connection-level progress;
2. preserving a useful read-service floor during accepted egress progress when
   TUN drops, send failures, and hard pause edges are absent;
3. keeping the existing hard guards for real residual pending pressure.

The next focused acceptance target remains `>30 Mbit/s` on safe1200
reverse-first P1 before returning to the `100+ Mbit/s` finish path.
