# Knife14eq Adaptive ACK Drain Results

Date: 2026-07-07

## Outcome

Knife14eq implemented and tested the adaptive ACK/window drain floor proposed
after Knife14ep, but the scoped VPS acceptance failed and did not validate a
throughput gain.

The failure shape changed materially from Knife14ep. The failing run had almost
no local downlink pressure, no TUN drops, no QUIC loss/blocking, and no
terminal pending. The dominant signal was a reverse-first no-data stall with
late remote data after local finish.

## Code Result

- Added a bounded adaptive ACK/window drain floor to `DownlinkCreditController`.
- The adaptive floor grows from observed clean local egress progress.
- The adaptive floor clamps back to the tiny one-MTU floor near high water or
  after consecutive no-egress-progress feedback.
- The relay read-credit pressure cap now receives the controller's adaptive
  floor instead of always using the static tiny floor.

## Local Gates

Passed:

- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib projected_payload_pressure --quiet`
- `cargo test --lib downlink_pressure_credit --quiet`
- `cargo test --lib deferred_ack_drain --quiet`
- `cargo test --lib relay_remote_ready_burst --quiet`
- `cargo test --lib --quiet`
- `cargo test --features harness --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `cargo build --release --quiet`
- `git diff --check`

Formatting note: whole-repo `cargo fmt --check` still reports historical
unrelated formatting drift, so only `src/client_tun.rs` was formatted with
`rustfmt --edition 2024`.

## Remote Focused Gates

The working tree was synced to `.27` with `.env`, `.git`, `target`, and tar
archives excluded. The following focused gates passed on `.27`:

- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib projected_payload_pressure --quiet`
- `cargo test --lib downlink_pressure_credit --quiet`
- `cargo build --release --quiet`

## VPS Attempts

### Attempt A

- tag: `knife14eq_adaptive_ack_drain_p1_30`
- remote bundle:
  `/tmp/conn/mvpn_knife14eq_adaptive_ack_drain_p1_30_usclient_suite_20260707_180232.tar.gz`
- result: preflight failed before tunnel P1 because
  `EXIT_TO_TARGET_IPERF_CHECK=1` was set without the matching exit SSH
  settings.
- useful signal: direct `.27 -> .77` reverse baseline was healthy at about
  `274 Mbit/s` receiver.

### Attempt B

- tag: `knife14eq_adaptive_ack_drain_p1_30b`
- local bundle:
  `/tmp/mini_vpn/knife14eq_adaptive_ack_drain_p1_30b/mvpn_knife14eq_adaptive_ack_drain_p1_30b_usclient_suite_20260707_180324.tar.gz`
- remote bundle:
  `/tmp/conn/mvpn_knife14eq_adaptive_ack_drain_p1_30b_usclient_suite_20260707_180324.tar.gz`

Baselines were healthy:

- direct `.27 -> .77`: forward receiver about `275 Mbit/s`, reverse receiver
  about `274 Mbit/s`.
- exit `.33 -> .77`: forward receiver about `282 Mbit/s`, reverse receiver
  about `283 Mbit/s`.

Tunnel reverse-first P1 failed:

- sender: `0.699 Mbit/s`
- receiver: `0.030 Mbit/s`
- iperf shape: mostly zero-throughput intervals, with tiny bursts around
  seconds 9-10 and 20-21.

Key clean signals:

- `tun_tx_dropped_delta=0`
- QUIC loss/congestion/blocking all zero.
- `terminal_pending_reap=0`
- `pending_at_close=0`
- `egress_at_close=0`
- `headroom_deferred_bytes=0`
- `pressure_credit_debt_bytes=0`
- `drop_credit_blocked_bytes=0`

Key failure signals:

- `throughput_shape: shape=no_data ... local_pressure=0 no_data=1`
- `downlink_backpressure: pause_edges=0 resume_edges=0 max_pressure_bytes=0`
- `downlink_flush: accepted_bytes=224536 send_queue_max=92288 may_recv_false=5`
- `tcp_reverse_window: payload_bytes=162347 accepted_bytes=162347 pending_max=0`
- `read_credit_limit_bytes_min=65536`
- `remote_batch_limit_bytes_min=65536`
- `relay_late_remote: post_finish_bytes=92288 post_finish_reads=4 local_finish_events=1`
- `relay_remote_timing: data_max_read_gap_ms=20491`
- `tuic_tcp_stream: data_read_gap_max_ms=20491`
- `tuic_stream_pending: data_pending_gap_max_ms=19922`
- attribution:
  `late_remote_after_local_finish+tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap+target_sender_stalled+tuic_stream_starved`

## Interpretation

Knife14eq did not reproduce the Knife14ep pressure-starvation failure. In this
run, local pressure stayed at zero and read credit never collapsed below the
old pressure floor (`65536` bytes). That means the new adaptive ACK/window
floor was not the active limiter in the failing window.

The actionable new evidence is the no-data branch:

- The target sender stalled while the path baselines were healthy.
- The data stream had long TUIC stream pending/read gaps without QUIC
  congestion, loss, or blocked frames.
- Local finish was observed before all remote payload had arrived, followed by
  late remote data (`92288` bytes) after local finish.
- Local egress accepted the small amount of remote data it received, and there
  was no pending buildup.

Therefore the next stage should not tune the pressure floor further. The next
stage should isolate why reverse data can stall with no local pressure, focusing
on local FIN ordering, read-only-after-local-finish behavior, and TUIC stream
pending/read wakeups.

## Next Plan

Knife14er should be a no-pressure reverse no-data / local-FIN-ordering stage:

1. Add deterministic tests or harness coverage around local FIN while remote
   payload is still legal and expected.
2. Add diagnostics that correlate `local_fin_sent`, `writer_done`,
   `read_only_after_local_finish`, last remote progress, and data stream
   pending/read gaps.
3. Ensure local FIN does not suppress relay read cadence or TUN egress when the
   remote stream can still deliver payload.
4. Repeat one scoped safe1200 reverse-first P1 only after the discriminator is
   in place.

## Acceptance Status

Not accepted.

Knife14eq improves deterministic controller coverage, but the VPS result did
not move throughput toward the `100+ Mbit/s` receiver target. Overall Knife14
remains around the 97% engineering-progress mark: the drop/pressure edge is
better understood, but the no-pressure reverse stall must be closed before the
remaining throughput gap can be attacked safely.
