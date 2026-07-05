# Knife14bh Stream-Gap A/B Results

Date: 2026-07-05

## Code Under Test

- Commit: `7e33d44`
- Stage patch: behavior-neutral TUIC stream pending diagnostics, reverse-only
  suite stop mode, and `MINI_VPN_TUN_RX_DRAIN_BUDGET` A/B gate.

## Local Gates

Passed:

- `cargo test --lib parse_tun_rx_drain_budget_allows_zero_and_bounds`
- `cargo test --lib tuic_tcp_stream_diag_tracks_rate_limited_pending_gaps`
- `cargo test --lib tuic_tcp_stream`
- `cargo test --lib tun_rx_drain`
- `cargo test --features harness loopback_try_recv_rx --lib`
- `cargo test --lib client_tun`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `git diff --check`
- `cargo clippy --all-targets --features harness -- -D warnings`
- non-sandbox `cargo test --lib`
- non-sandbox `cargo test --features harness --lib`

Notes:

- Sandbox `cargo test --lib` and `--features harness --lib` failed only on the
  known QUIC endpoint bind tests; both passed outside the sandbox.
- Root `cargo fmt --check` reported repo-wide historical formatting diffs and
  was not used as a Knife14bh gate.

## VPS Bundles

Default drain budget:

- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bh_default_usclient_suite_20260705_094918.tar.gz`
- Extracted:
  `/tmp/mini_vpn/knife14bh_default_20260705_094918/`
- Remote bundle:
  `/tmp/conn/mvpn_knife14bh_default_usclient_suite_20260705_094918.tar.gz`
- Env: `MINI_VPN_TUN_RX_DRAIN_BUDGET=8`,
  `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`

Drain disabled:

- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bh_drain0_usclient_suite_20260705_095120.tar.gz`
- Extracted:
  `/tmp/mini_vpn/knife14bh_drain0_20260705_095120/`
- Remote bundle:
  `/tmp/conn/mvpn_knife14bh_drain0_usclient_suite_20260705_095120.tar.gz`
- Env: `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`,
  `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`

Both runs used `.27` client, `.33` TUIC/sing-box exit, `.77` iperf3 target.
Preflight services were healthy and direct baselines were healthy.

## Baselines

Default run:

- `.27 -> .77`: forward receiver `274 Mbit/s`, reverse receiver `280 Mbit/s`
- `.33 -> .77`: forward receiver `286 Mbit/s`, reverse receiver `281 Mbit/s`

Drain0 run:

- `.27 -> .77`: forward receiver `279 Mbit/s`, reverse receiver `260 Mbit/s`
- `.33 -> .77`: forward receiver `280 Mbit/s`, reverse receiver `282 Mbit/s`

## A/B Result

Default drain budget `8`:

- Reverse P1: sender `0.245 Mbit/s`, receiver `0.035 Mbit/s`
- `tun_rx_drain`: `attempts=11 packets=53 tcp=53 budget_exhausted=6`
- Data-stream TUIC timing:
  `data_first_rx_max_ms=24517`, `data_read_gap_max_ms=13702`,
  `data_pending_gap_max_ms=24516`, `data_rx_bytes_max=209758`
- Relay timing:
  `data_first_read_max_ms=24517`, `data_max_read_gap_ms=13702`,
  `data_rx_bytes_max=209758`
- Local egress stayed clean:
  `tun_tx_dropped_delta=0`, runtime TUN drop `0`,
  `downlink_backpressure pause_edges=0`
- Close tail:
  `terminal_pending_reap=11264B`, `pending_at_close=11264B`
- Attribution:
  `terminal_pending_reap+pending_at_close+tuic_stream_first_byte_slow+tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_first_byte_slow+relay_remote_read_gap`

Drain disabled `0`:

- Reverse P1: sender `26.500 Mbit/s`, receiver `24.500 Mbit/s`
- `tun_rx_drain`: `attempts=0 packets=0`
- Data-stream TUIC timing:
  `data_first_rx_max_ms=4`, `data_read_gap_max_ms=3410`,
  `data_pending_gap_max_ms=1949`, `data_rx_bytes_max=99280544`
- Relay timing:
  `data_first_read_max_ms=3`, `data_max_read_gap_ms=3410`,
  `data_rx_bytes_max=99280544`
- Remaining local pressure:
  `downlink_backpressure pause_edges=11 resume_edges=11`,
  `send_queue_max=1048576`, `tun_tx_dropped_delta=1639`,
  runtime TUN drop `819`
- Close tail:
  `pending_at_close=5617B`, classified `active_no_send`;
  `terminal_pending_reap=0`
- Attribution:
  `local_tun_egress_drop+local_downlink_backpressure+pending_at_close+pending_close_active_no_send`

QUIC was clean in both runs:

- `max_lost_bytes_delta=0`
- `max_congestion_events_delta=0`
- no inherited congestion
- no QUIC flow-control blocked delta

## Conclusion

The Knife14bg opportunistic TUN RX drain cadence is not safe as a product
default. With the default budget `8`, clean reverse-first collapsed to
`0.035 Mbit/s` receiver and the data stream did not deliver useful bytes until
about `24.5s`.

Disabling the drain path restored useful downlink flow to `24.5 Mbit/s` and
removed the data-stream first-byte/pending stall from the data stream. This does
not complete Knife14, because the recovered run still hit local
downlink/TUN-egress pressure and remained below the P1 throughput target.

## Next Plan

Knife14bi should first remove the regression by changing the product/suite
default drain budget to `0`, while preserving the env-gated drain path for
future controlled experiments. The acceptance target for that patch is that the
default clean reverse-first run matches the drain0 shape instead of the
drain=8 collapse.

After that confirmation, the remaining branch is local TUN egress/downlink
backpressure under the drain-disabled path, not TUIC data-stream first-byte
stall.
