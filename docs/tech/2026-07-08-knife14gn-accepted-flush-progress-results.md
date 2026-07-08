# 2026-07-08 Knife14gn Accepted Flush Progress Results

## Goal

Continue T10 and T11 from the current Knife14 TODO:

1. Add TDD for useful local egress progress when downlink bytes are accepted by
   smoltcp.
2. Feed that progress into read-credit publishing without weakening the drop /
   hard-pause safeguards.
3. Run one focused safe1200 reverse-first P1 and answer whether the result
   exceeds `30 Mbit/s`.

Non-goals stayed unchanged: no VPS tuning, no iperf3 changes, no
MTU/PLPMTUD work, no stale-pool work, and no broad QUIC window changes.

## Code

- Commit: `97cb55e` (`fix(knife14gn): count accepted downlink flush progress`)
- Local TDD:
  - Red: `cargo test downlink_credit_controller_treats_accepted_flush_bytes_as_egress_progress -- --nocapture`
    initially failed because `note_flush_feedback_with_accepted` did not exist.
  - Green: the same test passed after the controller fix.
- Local gates:
  - `cargo test downlink_credit_controller -- --nocapture`
  - `cargo test pressure_credit -- --nocapture`
  - `cargo test relay_read_credit -- --nocapture`
  - `cargo test projected -- --nocapture`
  - `cargo test`
  - `cargo clippy --all-targets --features harness -- -D warnings`
  - `cargo build --release`
- Remote `.27` workdir:
  `/tmp/mini_vpn_knife14gn_97cb55e`
- Remote `.27` gates:
  - `.mini_vpn_commit = 97cb55e`
  - `cargo test downlink_credit_controller_treats_accepted_flush_bytes_as_egress_progress -- --nocapture`
  - `cargo build --release`

## TDD Result

The new test covers the case that Knife14gm left open: a flush can be headroom
limited but still successfully accept bytes into the local TCP send buffer.
That is useful local egress progress and must:

- reset `no_egress_progress_streak`;
- increment `egress_progress_generation` so read-credit publishers can wake;
- keep a useful read-credit cadence at the credit edge.

The production path now calls controller feedback after the `send_slice` result
is known, passing the accepted byte count. Zero-write and error branches still
report zero accepted bytes. The old `note_flush_feedback` wrapper is kept only
for tests that model observed send-queue drain directly.

## Acceptance Setup

Preflight checks:

- `.33` sing-box: `active`
- `.77` iperf3: `active`
- `.33` socket buffers:
  - `net.core.rmem_max = 16777216`
  - `net.core.wmem_max = 16777216`
  - `net.core.rmem_default = 1048576`
  - `net.core.wmem_default = 1048576`
- Direct `.27 -> .77` one-second checks during the suite:
  - forward receiver: `280 Mbit/s`
  - reverse receiver: `280 Mbit/s`

## VPS Result

Bundle:

- Remote:
  `/tmp/conn/mvpn_knife14gn_accepted_flush_progress_safe1200_p1_30_usclient_suite_20260708_200258.tar.gz`
- Local:
  `/tmp/mini_vpn/mvpn_knife14gn_accepted_flush_progress_safe1200_p1_30_usclient_suite_20260708_200258.tar.gz`

Focused safe1200 reverse-first P1:

- Sender: `18.8 Mbit/s`
- Receiver: `18.0 Mbit/s`
- Answer to the stage question: **No, this did not exceed `30 Mbit/s`.**

## Key Signals

Positive:

- The old local-pressure attribution is gone in this run:
  `throughput_shape=shape=low_average tail_collapse=0 local_pressure=0 no_data=0 stable_high=0`.
- `downlink_backpressure pause_edges=0 resume_edges=0`.
- `global_rx_receive pause_edges=0 resume_edges=0`.
- `local_write_pressure events=0`.
- `read_credit_pause_updates=0`.
- Downlink accepted and flushed useful bytes:
  `accepted_bytes=63042499`, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`, `tun_flush_deferred=0`.
- Pressure/drop debt stayed clear:
  `drop_credit_debt_bytes=0`, `pressure_credit_debt_bytes=0`,
  `pressure_credit_debt_paid_bytes=0`, `pressure_credit_blocked_bytes=0`.
- TUN drops stayed clear:
  `tun_tx_dropped_delta=0`.
- QUIC loss/congestion/blocking stayed clear:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_data_delta=0`, `max_tx_blocked_stream_delta=0`,
  `max_rx_blocked_data_delta=0`, `max_rx_blocked_stream_delta=0`.

Still failing:

- The tunnel remained burst/idle and low-average, with many zero one-second
  intervals and only `18.0 Mbit/s` receiver.
- The data stream still showed ordered-stream pending gaps:
  `data_read_gap_max_ms=3405`,
  `data_pending_gap_max_ms=3405`,
  `connection_stream_frames_pending=10`.
- The data stream received `67953396` bytes, so this is not the Knife14fu/gl
  no-data failure.
- Close-tail was not final-acceptance clean:
  `terminal_late_remote_payload=16896` and `egress_at_close=230746`, although
  `pending_at_close=0` and `terminal_pending_reap=0`.

## Interpretation

Knife14gn did what T10 was supposed to do: accepted downlink flush bytes now
feed the local progress/read-credit path, and the focused VPS run no longer
shows local pressure, pressure debt, drop debt, read-credit pauses, TUN drops,
or QUIC blocking as the primary limiter.

It did **not** restore `>30 Mbit/s`. The result moved the root away from local
pressure-credit constants and back to TUIC ordered-stream receive cadence:
mini_vpn has read credit available and accepts data once it arrives, but the
data stream repeatedly sits in `connection_stream_frames_pending` for up to
about `3.4s`.

## Next

Do not retune VPS, iperf3, MTU/PLPMTUD, stale pool, broad QUIC windows, or the
pressure-credit constants from this result.

Next code slice should add focused TDD/diagnostics around ordered TUIC stream
receive cadence and self-wake behavior when:

- read credit is open (`read_credit_pause_updates=0`);
- local pressure is clean;
- QUIC loss/blocking is clean;
- pending cause remains `connection_stream_frames_pending`;
- `conn_rx_stream_frames_since_read` and `conn_udp_rx_since_read` show transport
  progress since the last application read.

The next acceptance target remains first `>30 Mbit/s` on the same focused
safe1200 reverse-first P1.
