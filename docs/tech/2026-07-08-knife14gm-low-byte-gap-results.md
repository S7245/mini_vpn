# 2026-07-08 Knife14gm Low-Byte Ordered Gap Results

## Goal

Continue from Knife14gl T1 through T9:

1. Add and keep a deterministic TDD case for the `60704B` ordered-stream gap
   below the old `64KiB` ACK/window service gate.
2. Fix the local ACK-drain due predicate and relay supervisor polling gate.
3. Run one focused safe1200 reverse-first P1 acceptance and answer whether it
   exceeds `30 Mbit/s`.

Non-goals stayed unchanged: no VPS tuning, no iperf3 changes, no MTU/PLPMTUD
work, no stale-pool work, and no broad QUIC window changes.

## Code

- Commit: `9e50a32` (`fix(knife14gm): service low-byte ordered stream gaps`)
- Local tests:
  - `cargo test relay_ack_drain_hint -- --nocapture`
  - `cargo test --lib`
- Remote `.27` tests/build:
  - Temp workdir: `/tmp/mini_vpn_knife14gm_9e50a32`
  - Commit marker: `.mini_vpn_commit = 9e50a32`
  - `cargo test relay_ack_drain_hint -- --nocapture`
  - `cargo build --release`

`cargo clippy --lib -- -D warnings` was not a clean local gate because of two
pre-existing `collapsible_if` warnings outside this change.

## TDD Result

The new tests cover both layers that mattered for Knife14gl:

- `relay_ack_drain_hint_services_low_byte_ordered_stream_gap` proves a
  payload-shaped active ordered stream with `remote_to_global_rx_bytes=60704`
  and read-service activity requests ACK/window service below `64KiB`.
- `relay_ack_drain_hint_polling_includes_low_byte_active_read_service` proves
  the relay supervisor polling gate reaches the ACK hint branch for that same
  shape.

The existing guard test still passes: tiny `512B` control streams do not emit
ACK hints, and paused/zero-credit receive credit remains handled by the outer
remote-read-probe gate.

## Acceptance Setup

Preflight checks:

- `.33` sing-box: `active`
- `.77` iperf3: `active`
- `.33` socket buffers:
  - `net.core.rmem_max = 16777216`
  - `net.core.wmem_max = 16777216`
  - `net.core.rmem_default = 1048576`
  - `net.core.wmem_default = 1048576`

The first focused suite attempt failed before data-plane startup:

- Tag: `knife14gm_lowbyte_gap_safe1200_p1_30`
- Failure: `tuic auth finish: sending stopped by peer: error 0`
- Bundle:
  `/tmp/conn/mvpn_knife14gm_lowbyte_gap_safe1200_p1_30_usclient_suite_20260708_184841.tar.gz`

No config or service changes were made. A same-command retry was run.

## VPS Result

Retry bundle:

- Remote:
  `/tmp/conn/mvpn_knife14gm_lowbyte_gap_safe1200_p1_30_retry1_usclient_suite_20260708_185002.tar.gz`
- Local:
  `/tmp/mini_vpn/knife14gm_lowbyte_gap_safe1200_p1_30_retry1/mvpn_knife14gm_lowbyte_gap_safe1200_p1_30_retry1_usclient_suite_20260708_185002.tar.gz`

Focused safe1200 reverse-first P1:

- Sender: `23.2 Mbit/s`
- Receiver: `21.2 Mbit/s`
- Answer to the stage question: **No, this did not exceed `30 Mbit/s`.**

The run did restore data movement compared with Knife14gl's `16.2 Kbit/s`
receiver no-data result, but it stayed in the old low-average band.

## Key Signals

Positive:

- `first_rx_ms=3` on the data stream.
- `remote_to_global_rx_bytes=79482037` on the data stream.
- `relay_gap_hints: events=204`.
- `ack_drain_hint_due=33` already visible in the first live data-stream
  sample, and post-run ACK hints continued.
- `pending_at_close=0`.
- `terminal_pending_reap=0`.
- `terminal_late_remote_payload=0`.
- `tun_tx_dropped_delta=0`.
- QUIC loss/congestion/blocking deltas were clean:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_data_delta=0`, `max_tx_blocked_stream_delta=0`,
  `max_rx_blocked_data_delta=0`, `max_rx_blocked_stream_delta=0`.

Still failing:

- `throughput_shape=shape=low_average tail_collapse=0 local_pressure=1 no_data=0 stable_high=0`.
- The data stream remained burst/idle:
  `data_read_gap_max_ms=3598`,
  `data_pending_gap_max_ms=3005`,
  `connection_stream_frames_pending=20`.
- `pressure_credit_debt` returned after the iperf window around
  `projected_payload_credit_edge`, while local send capacity stayed available
  and no TUN drops were reported.

## Interpretation

Knife14gm closed the specific Knife14gl low-byte ACK hint hole. The current
branch is no longer in the `0.046 Mbit/s` no-data shape and no longer stalls at
`60704B` below the 64KiB service threshold.

It did **not** restore `>30 Mbit/s`, let alone `100+ Mbit/s`. The next branch is
back to the local pressure-credit / egress-progress controller path: mini_vpn
can now move data and emit ACK/window service, but it still delivers in large
bursts separated by multi-second ordered-stream pending gaps.

## Next

Do not retune VPS, iperf3, MTU/PLPMTUD, stale pool, or broad QUIC windows from
this result.

Next code slice should add TDD around local pressure-credit repayment and
read-credit publishing when there is useful egress progress but the data stream
is still `connection_stream_frames_pending`. The target acceptance for the next
slice should first be `>30 Mbit/s` on the same focused reverse-first P1, then
resume the `100+ Mbit/s` path.
