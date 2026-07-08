# Knife14ew egress-earned payload credit results

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Goal

Implement and test the recommended egress-earned payload credit branch:

- relay read-credit should be earned from actual local egress progress;
- payload read-credit should not expand bounded local pending;
- ACK/window drain should remain available without hard-stopping QUIC receive;
- local pressure close to high water should still shrink quickly.

The acceptance target remained clean reverse-first P1 receiver `100+ Mbit/s`
with `pending_at_close=0`, `terminal_pending_reap=0`,
`tun_tx_dropped_delta=0`, and QUIC loss/blocking `0`.

## Local changes tested

The implementation added a bounded payload-token reservoir to
`DownlinkCreditController`:

- `egress_payload_credit_bytes` is earned from observed local egress progress
  in `note_flush_feedback`;
- token credit is only published in the pressure region and is capped by the
  hard pause headroom;
- projected payload reads spend tokens only when projected local pressure is at
  or above the flush threshold;
- paused relay credit and bounded staging still override token availability;
- `tcp-downlink-flush` and the suite parsers now report
  `egress_payload_credit_bytes`.

Focused TDD covered:

- earning payload tokens from egress progress;
- spending tokens only under projected pressure;
- tokens not overriding pause/staging guards;
- flush aggregate formatting for the new token field.

## Local gates

Passed:

- `cargo test --lib payload_tokens --quiet`
- `cargo test --lib tokens_do_not --quiet`
- `cargo test --lib tcp_downlink_flush_aggregate_formats_progress_signal --quiet`
- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib tun_rx_drain --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `cargo test --lib --quiet`
- `cargo test --features harness --quiet`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `cargo build --release --quiet`
- `git diff --check`

Notes:

- `cargo fmt --check` still reports historical formatting differences outside
  this stage's touched Rust file. To avoid unrelated churn, this stage only
  formatted and checked `src/client_tun.rs` with edition `2024`.
- The repository secret scan only hit existing placeholders and safety text, no
  real credentials.

## Remote focused gates

`.27` passed:

- `cargo test --lib payload_tokens --quiet`
- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib relay_read_credit --quiet`
- low-RTT probe self-test
- US-client suite self-test
- release build

## VPS run

Local bundle:
`/tmp/mini_vpn/knife14ew_egress_tokens_p1_30/mvpn_knife14ew_egress_tokens_p1_30_usclient_suite_20260707_220735.tar.gz`

Remote bundle:
`/tmp/conn/mvpn_knife14ew_egress_tokens_p1_30_usclient_suite_20260707_220735.tar.gz`

Preflight was healthy:

- client `.27 -> .77` forward receiver: `281 Mbit/s`
- client `.27 -> .77` reverse receiver: `275 Mbit/s`
- exit `.33 -> .77` forward receiver: `283 Mbit/s`
- exit `.33 -> .77` reverse receiver: `283 Mbit/s`

Tunnel result failed acceptance badly:

- iperf sender: `0.979 Mbit/s`
- iperf receiver: `0.046 Mbit/s`
- shape: `no_data`, `local_pressure=0`, `tail_collapse=0`

## Key evidence

The new token mechanism was wired and did earn credit:

- `egress_payload_credit_bytes=122727`
- `drain_credit_granted_bytes=297656`
- `pending_total_max=0`
- `pending_max=0`
- `headroom_deferred_bytes=0`
- `pressure_credit_debt_bytes=0`
- `pressure_credit_blocked_bytes=0`

Local egress and close surfaces were clean:

- `tun_tx_dropped_delta=0`
- runtime TUN egress drop events: `0`
- TUN egress feedback pause edges: `0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `pending_at_close=0`
- `egress_at_close=0`
- `terminal_pending_reap=0`

QUIC client-side loss/blocking stayed clean:

- `max_lost_bytes_delta=0`
- `max_congestion_events_delta=0`
- `max_tx_blocked_data_delta=0`
- `max_tx_blocked_stream_delta=0`
- `max_rx_blocked_data_delta=0`
- `max_rx_blocked_stream_delta=0`
- safe1200 remained active with `dg_max=Some(1166)` and
  `plpmtud(sent=0,lost=0,black_holes=0)`.

The failure moved away from local pressure and into stream starvation:

- `relay_remote_timing max_read_gap_ms=30002`
- data stream `data_max_read_gap_ms=23974`
- `tuic_stream_pending max_pending_gap_ms=30000`
- data stream `data_pending_gap_max_ms=23247`
- `relay_gap_hints events=41`, `max_cadence_floor=19200`
- attribution:
  `late_remote_after_local_finish+tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap+target_sender_stalled+tuic_stream_starved`

The raw stream evidence is especially important:

- relay data stream stayed at `remote_to_global_rx_bytes=42360` for roughly
  17s, then advanced to `297656`, then to `333240`;
- `read_credit_limit_bytes_min=65536`, so this was not a pressure floor
  collapse;
- `conn_udp_rx_since_read` and `conn_rx_stream_frames_since_read` grew while the
  same TUIC TCP stream read stayed pending.

## Classification

Environment issue: rejected. Direct and exit-to-target baselines were healthy.

Local downlink pending/backpressure issue: rejected for this run. Pending,
headroom deferral, pressure debt, TUN drops, close-tail pending, and send-slice
errors were all zero or clean.

Payload-token branch: locally correct but insufficient. The token reservoir
earned credit and remained bounded, but the run was pressure-free; payload
credit did not address the stream starvation shape.

Short ACK cadence boost: still insufficient. Gap hints fired and published a
`19200B` cadence floor, but the stream remained pending for multi-second to
30s windows.

## Decision

Do not continue by raising payload-token caps, cadence floor, egress pacer
budget, TUN queue length, MTU/PLPMTUD, stale-pool logic, iperf3, or sing-box.

Knife14ew rejects the current "egress-earned payload credit + one-shot cadence
hint" as the final 3% solution. The next useful branch should target the
pressure-free starvation evidence:

1. add a deterministic/local harness around TUIC stream pending diagnostics if
   possible, or at least enhance runtime diagnostics to distinguish QUIC
   receive reorder/loss from missing ACK/window uplink service;
2. add per-stream receive progress signals beyond connection-level
   `rx_stream_frames`, because connection-level stream frames can increase while
   the active `RecvStream` remains pending;
3. inspect/adjust the TUN ACK/window service path only with new evidence:
   pressure-free reverse stalls with `pending_total_max=0`,
   `read_credit_limit_bytes_min=65536`, low TUN RX packet counts, and
   target-sender-stalled shape;
4. keep the payload-token code only if the next review finds it harmless and
   useful as bounded pressure-path machinery; otherwise back it out before the
   next acceptance branch.

This stage advanced the overall diagnosis but did not advance acceptance
throughput. The remaining work is no longer well described as "increase local
downlink credit"; it is a stream starvation / ACK-window service discriminator.
