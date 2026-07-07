# Knife14en QUIC Safe1200 Results

Date: 2026-07-07

## Stage Goal

Discriminate whether the latest reverse-first TCP stalls are caused by QUIC
MTU discovery / path black-hole behavior, or by mini_vpn local downlink egress
credit and backpressure.

## Code Changes Under Test

- Added `MtuPolicy` in `src/quic.rs`.
  - `default`: existing 1280 initial/min MTU with PLPMTUD enabled.
  - `safe1200`: 1200 initial/min MTU with PLPMTUD disabled.
- Added `MINI_VPN_TUIC_MTU_MODE` / `MINI_VPN_TUIC_MTU_POLICY` config plumbing
  in `src/tuic.rs`.
- Added QUIC delivery counters to TUIC TCP stream pending diagnostics:
  `conn_udp_rx`, `conn_rx_stream_frames`, ACK frame counters, and PLPMTUD /
  black-hole counters.

## Local And Remote Gates

- Local:
  - `cargo test --lib --quiet` passed: 367 tests.
  - `bash scripts/knife14b-lowrtt-probe.sh --self-test` passed.
  - `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test` passed.
  - `cargo build --release --quiet` passed.
  - `git diff --check` passed.
- `.27` focused remote gates passed:
  - `cargo test --lib mtu_policy --quiet`
  - `cargo test --lib tuic_tcp_stream_diag --quiet`
  - `cargo test --lib format_quic_stats_line_includes_flow_and_congestion_signals --quiet`
  - `cargo build --release --quiet`
- `.33` sing-box and `.77` iperf3 were active and time-synchronized before
  the run.

## VPS Acceptance

- Label: `knife14en_quic_safe1200_tail12`
- Bundle:
  `/tmp/mini_vpn/knife14en_quic_safe1200_tail12/mvpn_knife14en_quic_safe1200_tail12_usclient_suite_20260707_160539.tar.gz`
- Remote bundle:
  `/tmp/conn/mvpn_knife14en_quic_safe1200_tail12_usclient_suite_20260707_160539.tar.gz`
- Probe:
  reverse-first P1, duration 30s, close-tail settle 12s, `MINI_VPN_TUIC_MTU_MODE=safe1200`.

## Result

- Direct baseline stayed healthy:
  - `.27 -> .77`: `334/282 Mbit/s`
  - `.77 -> .27`: `310/282 Mbit/s`
- Tunnel reverse-first P1 stayed low:
  - sender: `24.3 Mbit/s`
  - receiver: `23.1 Mbit/s`
  - shape: `low_average`, not stable high.
- `safe1200` was applied:
  - startup logged `QUIC MTU policy=safe1200`
  - `dg_max=Some(1166)`
  - `plpmtud(sent=0,lost=0,black_holes=0)`

## Key Signals

- QUIC/path loss branch remains clean:
  - `max_lost_bytes_delta=0`
  - `max_congestion_events_delta=0`
  - no TX/RX flow-control blocked frames.
- New transport delivery counters prove the data QUIC stream was receiving
  transport frames:
  - final data connection sample: `frames(rx_stream=68905,rx_ack=102,tx_ack=21099)`
  - final UDP receive sample: `udp_rx=104392/148771086B`
  - pending lines showed non-zero `conn_rx_stream_frames_since_read` during
    gaps while PLPMTUD stayed zero.
- The current failure shifted back to local downlink pressure:
  - `downlink_backpressure pause_edges=4 resume_edges=3`
  - `max_pending_bytes=725572`
  - `send_queue_max=556664`
  - `headroom_limited=15691`
  - `headroom_deferred_bytes=4018847472`
  - `pressure_credit_blocked_bytes=1907512`
  - `pending_total_max=327675`
- Lifecycle and syscall surfaces stayed clean:
  - `terminal_pending_reap=0`
  - `pending_at_close=0`
  - `egress_at_close=0`
  - `tun_tx_dropped_delta=0`
  - `send_slice_zero=0`
  - `send_slice_errors=0`
  - `tun_flush_failures=0`

## Interpretation

`safe1200` is not the final fix. It does, however, reject the hypothesis that
the latest no-data/low-average failure is primarily PLPMTUD black-hole behavior:
PLPMTUD was disabled and clean, while QUIC stream frames continued to arrive.

The next root is mini_vpn local downlink pressure policy. Relay read-credit now
limits individual batches (`remote_batch_limit_bytes_min=19200`,
`remote_batch_limited=343`), but local headroom/pressure credit still allows
the system to oscillate into `may_recv=false`, large headroom deferrals, and
active pending.

## Reusable Rule

Do not keep changing QUIC MTU or PLPMTUD policy for this Knife14 branch unless
new evidence contradicts this run. The next code patch should redesign the
local downlink credit loop so read-credit, flush budget, and headroom debt are
coupled around observed egress progress, rather than treating headroom
deferrals as a passive accounting signal.
