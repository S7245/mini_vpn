# Knife14bi Drain Default-Off Results

Date: 2026-07-05

## Code Under Test

- Local behavior commit: `76af8dc`
- `.27` repo commit during VPS attempt: `76af8dc`
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bi_default_usclient_suite_20260705_111824.tar.gz`
- Extracted locally:
  `/tmp/mini_vpn/knife14bi_default_20260705_111824/`

## Local Gates

Passed before the behavior commit:

- `cargo test --lib parse_tun_rx_drain_budget_allows_zero_and_bounds`
- `cargo test --lib tun_runtime_config_defaults_match_stage9_behavior`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `git diff --check`
- `cargo clippy --all-targets --features harness -- -D warnings`
- non-sandbox `cargo test --lib`
- non-sandbox `cargo test --features harness --lib`

## VPS Attempt 1: Startup Failure

The default scoped reverse-first P1 suite did not reach the throughput probe.
`client-tun` failed during TUIC startup:

```text
连接 TUIC 出口失败（启动中止）: Io(Custom { kind: Other, error: "tuic auth finish: sending stopped by peer: error 0" })
```

Evidence collected:

- `.27` pulled and built `76af8dc` with a clean git status.
- Suite env showed `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`.
- `client-tun` startup log confirmed
  `TUN RX drain budget: 0 packets/pass`.
- `.27 -> .77` direct baselines were healthy:
  - forward receiver about `279 Mbit/s`;
  - reverse receiver about `280 Mbit/s`.
- `.33 -> .77` exit-to-target baselines were healthy:
  - forward receiver about `279 Mbit/s`;
  - reverse receiver about `286 Mbit/s`.
- `.33` sing-box was active, had `NRestarts=0`, passed config check, and was
  listening on UDP `8443`.
- `.33` TUIC config summary matched expected shape:
  listen `:::8443`, one user, UUID length `36`, password length `16`,
  ALPN `h3`, server name `example.com`.
- `.33` server certificate was valid for the current date and issued by
  `Dev VPN CA`; `.27` had the corresponding CA file.
- A no-secret exact comparison reported:
  `uuid_match=1`, `password_match=1`, `sni_match=1`, `alpn_match=1`.

## Result

The first bundle is not throughput evidence. It only proves the Knife14bi
default config reached startup with drain disabled.

The failed startup matches the earlier Knife14bf TUIC auth-finish environment
failure shape: sing-box is active and credentials match, but the client is
closed by the peer before a useful TUIC data-plane session is established.

## VPS Attempt 2: Restart Then Rerun

After the failed startup, `.33` sing-box was restarted once. The rerun used the
same default scoped reverse-first suite, without overriding
`MINI_VPN_TUN_RX_DRAIN_BUDGET`.

- Code under test: `0794b8f` docs head, binary unchanged from `76af8dc`.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bi_default_rerun_usclient_suite_20260705_113246.tar.gz`
- Extracted locally:
  `/tmp/mini_vpn/knife14bi_default_rerun_20260705_113246/`

Startup and environment:

- `.33` restart cleared the TUIC auth-finish startup failure.
- `.33` log showed TUIC inbound connections from `.27` to `.77:5201` and no
  service-side TUIC error in the rerun window.
- `.27 -> .77` direct baselines were healthy:
  - forward receiver about `266 Mbit/s`;
  - reverse receiver about `275 Mbit/s`.
- `.33 -> .77` exit-to-target baselines were healthy:
  - forward receiver about `281 Mbit/s`;
  - reverse receiver about `281 Mbit/s`.
- Startup confirmed `TUN RX drain budget: 0 packets/pass`.
- Runtime summary confirmed `tun_rx_drain attempts=0 packets=0`.

Throughput result:

- Reverse P1 sender: `0.280 Mbit/s`.
- Reverse P1 receiver: `0.017 Mbit/s`.

Key attribution:

- `downlink_backpressure pause_edges=0`, `global_rx_pressure events=0`, and
  `local_write_pressure events=0`.
- `downlink_flush accepted_bytes=84656`, `no_send_capacity=0`,
  `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`, and
  `tun_flush_deferred=0`.
- `tun_tx_dropped_delta=0` and runtime TUN egress `drop_delta_total=0`.
- `terminal_pending_reap=0` and `pending_at_close=0`.
- QUIC summary remained clean: no loss delta, no congestion delta, no tx/rx
  blocked delta, `min_cwnd=12000`.
- Data stream first RX was fast (`3ms`), but then stream progress stalled:
  - `relay_remote_timing data_rx_bytes_max=84652`;
  - `relay_remote_timing data_max_read_gap_ms=17101`;
  - `tuic_stream_pending data_pending_gap_max_ms=17097`;
  - parser attribution:
    `tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap`.

Comparison to the Knife14bh drain0 accepted shape:

- Knife14bh drain0 receiver was `24.5 Mbit/s`, with data stream
  `data_rx_bytes_max=99280544`, `data_max_read_gap_ms=3410`, and
  `data_pending_gap_max_ms=1949`.
- Knife14bi restart rerun receiver was `0.017 Mbit/s`, with data stream
  `data_rx_bytes_max=86060`, `data_max_read_gap_ms=17101`, and
  `data_pending_gap_max_ms=17097`.

## Updated Result

Knife14bi successfully removed the default TUN RX drain regression from the
runtime path, but the default path did not reproduce the earlier Knife14bh
drain0 throughput.

The second failure is not explained by close-drain, terminal pending, local TUN
egress drops, downlink backpressure, or QUIC loss/blocking. The active branch is
now TUIC TCP stream starvation / remote-read scheduling: the data stream starts
quickly, but receives only tens of KiB over the 30s reverse-first window.

## Next Plan

Do not change mini_vpn data-plane code until the next stage plan is confirmed.

Proposed Knife14bj plan:

1. Ground the TUIC TCP stream read loop and sing-box interaction path.
2. Add a focused spec for data-stream starvation that distinguishes:
   - upstream server not sending;
   - TUIC stream read task not being polled;
   - stream flow-control/receive-window stall not visible in current QUIC
     counters;
   - local TCP ACK/window behavior not represented by downlink pending metrics.
3. Add deterministic tests or harness instrumentation for stream-read progress
   accounting before another VPS run.
4. Add one minimal diagnostic patch if the code review finds a missing
   progress counter, then rerun only the scoped reverse-first P1 suite.
