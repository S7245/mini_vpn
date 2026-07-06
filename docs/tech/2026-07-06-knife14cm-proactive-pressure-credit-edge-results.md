# Knife14cm Proactive Pressure Credit Edge Results

Date: 2026-07-06

## Code

- Commit: `e8f0a50`
- Scope: pressure credit debt can now be installed at the tx-queue credit
  edge, before the downlink backpressure pause edge. Repeated observations are
  idempotent while the same debt is active, and diagnostics distinguish
  `pressure_credit_edge` from `pressure_pause_edge`.

## Local Gates

- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `cargo test tun_rx_drain --lib`
- `cargo test close --lib`
- `cargo test reap --lib`
- `cargo test pending_downlink_close_deferral --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## VPS Runs

The first two current-binary runs did not exercise the target branch:

- `/tmp/mini_vpn/knife14cm_pressure_credit_edge_20260706_1905/mvpn_knife14cm_pressure_credit_edge_usclient_suite_20260707_030514.tar.gz`
  collapsed as `no_data` at `0.315/0.055 Mbit/s`.
- `/tmp/mini_vpn/knife14cm_pressure_credit_edge_retry_20260706_1908/mvpn_knife14cm_pressure_credit_edge_retry_usclient_suite_20260707_030838.tar.gz`
  also collapsed as `no_data` at `0.175/0.017 Mbit/s`.

Both had healthy direct baselines and clean local egress counters, so they were
not useful for judging the pressure-credit change.

An old-code A/B using the Knife14cl source restored the expected pressure
branch:

- Bundle:
  `/tmp/mini_vpn/knife14cl_ab_current_path_20260706_1912/mvpn_knife14cl_ab_current_path_usclient_suite_20260707_031259.tar.gz`
- Tunnel P1: `21.5/20.8 Mbit/s`.
- Final pressure branch: `pending=524906`, `send_queue=892928`, and
  `tcp-egress-credit-debt reason=pressure_edge installed_bytes=25194`.

After restoring the Knife14cm source on `.27`, `touch src/client_tun.rs` was
required before rebuilding so cargo would not reuse stale artifacts. The valid
current run was:

- Bundle:
  `/tmp/mini_vpn/knife14cm_pressure_credit_edge_valid_20260706_1916/mvpn_knife14cm_pressure_credit_edge_valid_usclient_suite_20260707_031619.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cm_pressure_credit_edge_valid_20260706_1916_local/`
- Binary hash:
  `888e4ec041d764cd4aa68fdf1efb04b5efc0d4aea323624d6b698c1f6a9b8b49`

Baselines were healthy:

- `.27 -> .77`: `316/276 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `303/280 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `324/268 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `309/284 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 failed acceptance:

- `iperf_sender_mbps=20.400`
- `iperf_receiver_mbps=19.100`
- `throughput_shape=low_average local_pressure=1 no_data=0`

## Clean Signals

- The valid current run was not a no-data stream-starvation run.
- Direct baselines stayed healthy.
- QUIC loss/congestion/blocking stayed zero in the relevant window.
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `terminal_late_remote_payload_events=0`
- `.33` log checks showed no current TUIC `fail auth`; observed error lines
  were VLESS/REALITY public scan noise or stream-cancel lines from the test.

## Knife14cm-Specific Signals

The patch did install pressure debt at the intended earlier edge:

```text
tcp-egress-credit-debt reason=pressure_credit_edge installed_bytes=24576
generation=2 debt_bytes=24576 max_pending=9074 max_tx_queue=892928
```

However, this did not pause or slow remote receive. The next remote payloads
were read while the local send queue stayed pinned at the credit edge:

```text
accepted_bytes=0 pending=24562 send_queue=892928
accepted_bytes=0 pending=58354 send_queue=892928
accepted_bytes=0 pending=91505 send_queue=892928
```

The run then hit the pause/drop edge:

- `tcp-egress-credit-debt reason=pressure_pause_edge installed_bytes=17514`
- `pressure_credit_debt_bytes=42090`
- `pressure_credit_debt_paid_bytes=0`
- `pressure_credit_blocked_bytes=24576`
- `drop_credit_debt_bytes=169856`
- `drop_credit_debt_paid_bytes=196608`
- `drop_credit_blocked_bytes=393216`
- `tun_tx_dropped_delta=4056`
- `drop_events=3`
- `pause_edges=2 resume_edges=1`
- `pending_total=541802`
- `send_queue_max=892928`
- `may_recv_false=7865`
- `headroom_limited_calls=7922`
- `headroom_deferred_bytes=2037083173`
- `tcp-deferred-close-pending ... pending=541802`

## Interpretation

Knife14cm improved the observability and timing of pressure-debt installation,
but it did not repair the throughput limiter. The valid run proves that
installing debt at `pressure_credit_edge` alone is insufficient because the
receive side can continue reading remote payloads after debt is active. Debt
then remains unpaid while the local send queue is pinned at `892928B`, and
pending grows until close-time deferral exposes the backlog.

The remaining root is still the mini_vpn local downlink pressure/recovery
algorithm, not stale pool slots, iperf3, sing-box auth, QUIC loss/congestion,
the egress pacer, or hidden close/reap accounting.

## Next

Knife14cn should make active drop/pressure credit debt participate in the
downlink receive-window decision. A debt-active flow at the credit edge should
stop accepting more remote payload until observed local egress progress repays
the debt or the pressure state drops below the recovery threshold. This should
be tested as a core `client_tun` behavior, not as a script-only workaround, and
the next VPS proof should show either lower `send_queue_max`/pending or
non-zero pressure-debt repayment before close.
