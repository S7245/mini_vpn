# Knife14eu ACK/window discriminator results

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Goal

Run one safe1200 reverse-first P1 after adding FIN-boundary relay diagnostics
so the next failure can be classified without another threshold-tuning loop.

The target remained clean reverse-first P1 receiver `100+ Mbit/s`, with clean
close/reap accounting, no TUN drops, and no QUIC loss/blocking.

## Local changes tested

Code and scripts were extended with discriminator-only observability:

- relay live/close diagnostics now include local finish count, first local
  finish after first remote read, remote read gap before local finish, and
  remote read gap after local finish;
- the low-RTT probe parser summarizes those FIN-boundary timing fields;
- the US-client suite includes relay timing and TUIC stream pending/polling
  summary lines in the probe report.

Local gates passed before the VPS run:

- `cargo test --lib relay_live_diag --quiet`
- `cargo test --lib --quiet`
- `cargo test --features harness --quiet`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `cargo build --release --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Remote `.27` focused gates also passed:

- `cargo test --lib relay_live_diag --quiet`
- `cargo test --lib downlink_credit_controller --quiet`
- low-RTT probe self-test
- US-client suite self-test
- release build

## VPS run

Local bundle:
`/tmp/mini_vpn/knife14eu_ack_window_discriminator_p1_30/mvpn_knife14eu_ack_window_discriminator_p1_30_usclient_suite_20260707_193254.tar.gz`

Remote bundle:
`/tmp/conn/mvpn_knife14eu_ack_window_discriminator_p1_30_usclient_suite_20260707_193254.tar.gz`

Preflight was healthy:

- client `.27 -> .77` forward receiver: `282 Mbit/s`
- client `.27 -> .77` reverse receiver: `279 Mbit/s`
- exit `.33 -> .77` forward receiver: `275 Mbit/s`
- exit `.33 -> .77` reverse receiver: `297 Mbit/s`

Tunnel result failed acceptance:

- iperf sender: `20.8 Mbit/s`
- iperf receiver: `19.8 Mbit/s`
- shape: `low_average`, `local_pressure=1`, `no_data=0`,
  `tail_collapse=0`

## Key evidence

The FIN-ordering branch is not supported by this run:

- `relay_late_remote post_finish_bytes=0 post_finish_reads=0`
- `local_finish_events=0`
- `data_max_read_gap_before_finish_ms=7002`
- `data_max_read_gap_after_finish_ms=0`
- `first_local_finish_after_first_remote_read_max_ms=0`

The TUIC/QUIC path stayed clean:

- QUIC loss/congestion/blocking: `0`
- `rx_blocked(data=0,stream=0)`
- safe1200 active with `dg_max=Some(1166)` and
  `plpmtud(sent=0,lost=0,black_holes=0)`
- data connection had `frames(rx_stream=62356)` and
  `udp_rx=93890/134171834B`

The stream still had large pre-FIN read/pending gaps while QUIC frames existed:

- `tuic_tcp_stream data_read_gap_max_ms=7002`
- `tuic_stream_pending data_pending_gap_max_ms=7001`
- `tuic_stream_polling data_poll_gap_max_ms=250`
- data stream received `74939202B` over `4957` reads

The local pressure loop returned:

- `downlink_backpressure pause_edges=3 resume_edges=2`
- `max_pending_bytes=437267`
- `max_tx_queue_bytes=557386`
- `max_total_pressure_bytes=887266`
- `may_recv_false=14046`
- `headroom_deferred_bytes=41455269`
- `pressure_credit_blocked_bytes=2257002`

Lifecycle/close accounting stayed mostly clean, but the close tail still needs
attention:

- summary reported `terminal_pending_reap=0`, `pending_at_close=0`,
  `egress_at_close=0`
- post-run log also showed `tcp-deferred-close-pending ... pending=128254`
  after `remote_shutdown_failed`, followed by close-drain flush credit; this
  was not a terminal reap event, but it is still a tail-drain symptom to keep
  visible
- `tun_tx_dropped_delta=0`, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`

## Classification

Environment issue: rejected for this run. Direct and exit-to-target baselines
were healthy and services were active.

QUIC path issue: rejected as the primary root. QUIC loss, congestion, blocking,
and PLPMTUD black holes remained zero.

FIN/half-close ordering issue: rejected as the next code path. There were no
local finish events on the data stream and no remote payload/read gap after
local finish.

Local downlink pressure: still present. The run returned to the familiar
low-average burst shape with `may_recv_false`, headroom deferral, and
backpressure edges.

ACK/window cadence / TUIC stream wakeup: still implicated. The largest data
stream read gap happened before local finish, while QUIC stream frames and UDP
bytes continued to arrive. The existing TUN RX gap-hint path
(`budget=240` packets in the log) was not enough to prevent multi-second
sender stalls.

## Decision

Do not implement the optional FIN-deferral A/B from the spec. Knife14eu shows
the data-stream gap is pre-FIN, not post-FIN.

Do not continue by only tuning headroom or MTU constants. The valid next code
stage should split the controller architecture:

1. local payload egress pressure loop: keep pending/send_queue under the TUN
   edge and shrink payload staging fast near high water;
2. ACK/window cadence loop: when stream pending/read gaps grow while QUIC is
   clean, grant bounded multi-MTU ACK/window drain and prioritize TUN RX drain
   without allowing unbounded local pending growth;
3. feedback join: grow ACK/window cadence only from observed clean egress and
   shrink immediately on TUN drop, no-progress egress, or pressure edge.

This stage did not complete throughput acceptance. It completed the
discriminator goal by rejecting FIN deferral and narrowing the final work to a
split ACK/window cadence controller.
