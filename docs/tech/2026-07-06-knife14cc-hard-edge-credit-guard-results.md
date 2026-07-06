# Knife14cc Hard-Edge Credit Guard Results

Date: 2026-07-06

## Code Under Test

- Commit: `7d1f47f` (`fix(knife14cc): guard drain credit below hard pause edge`)
- Branch: `codex/knife14d-downlink-reap-open`
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)

## Runs

Run A:

- Suite tag: `knife14cc_hard_edge_guard`
- Bundle:
  `/tmp/mini_vpn/knife14cc_hard_edge_guard_20260706_1606/mvpn_knife14cc_hard_edge_guard_usclient_suite_20260707_000531.tar.gz`
- Direct baselines were healthy:
  - `.27 -> .77` reverse receiver: `279 Mbit/s`
  - `.33 -> .77` reverse receiver: `287 Mbit/s`
- Reverse-first P1 collapsed:
  - sender: `0.349 Mbit/s`
  - receiver: `0.033 Mbit/s`
  - `throughput_shape=shape=no_data`
- Clean local/transport surfaces:
  - `tun_tx_dropped_delta=0`
  - QUIC loss/congestion/blocking deltas: `0`
  - `send_slice_zero=0`
  - `send_slice_errors=0`
  - `tun_flush_failures=0`
  - `pending_at_close=0`
  - `terminal_pending_reap=0`
- TUIC stream signal:
  - `tcp_pool: opens=2 conns=0`
  - data stream `first_rx_ms=6`
  - data stream `data_rx_bytes_max=260144`
  - data stream `data_read_gap_max_ms=20514`

Run B:

- Suite tag: `knife14cc_repeat`
- Bundle:
  `/tmp/mini_vpn/knife14cc_repeat_20260706_1611/mvpn_knife14cc_repeat_usclient_suite_20260707_001035.tar.gz`
- Direct baselines were healthy:
  - `.27 -> .77` reverse receiver: `281 Mbit/s`
  - `.33 -> .77` reverse receiver: `282 Mbit/s`
- Reverse-first P1 repeated the collapse:
  - sender: `0.245 Mbit/s`
  - receiver: `0.034 Mbit/s`
  - `throughput_shape=shape=no_data`
- Clean local/transport surfaces:
  - `tun_tx_dropped_delta=0`
  - QUIC loss/congestion/blocking deltas: `0`
  - `send_slice_zero=0`
  - `send_slice_errors=0`
  - `tun_flush_failures=0`
  - `pending_at_close=0` in the probe summary
  - `terminal_pending_reap=0`
- TUIC stream signal:
  - `tcp_pool: opens=2 conns=0`
  - data stream `first_rx_ms=17880`
  - data stream `data_rx_bytes_max=258152`
  - data stream `data_read_gap_max_ms=13705`

## Exit Evidence

The `.33` sing-box evidence showed current-window TUIC service activity and
`connection download closed: stream 4 canceled by remote with error code 0`
near the failed reverse runs. It did not show current-window TUIC `fail auth`.
This points at stream/cancel or target-sender stall behavior, not a password,
clock skew, or sing-box configuration issue.

## Interpretation

The hard-edge guard itself did not engage in the failing clean window:
`hard_edge_guard_limited=0` and `hard_edge_guard_deferred_bytes=0` in both
pool=1 failures. The collapse happened before local egress pressure became the
limiter.

The repeated discriminator is that both iperf reverse control and data streams
were opened on `conn=0`. With one TUIC TCP connection, the data stream either
received only a tiny amount or waited almost 18 seconds for first payload. This
made the run look like an egress regression even though egress metrics stayed
clean.

## Follow-up A/B

Without changing the `7d1f47f` binary, the suite was rerun with
`MINI_VPN_TUIC_TCP_POOL=2`:

- Suite tag: `knife14cd_pool2_ab`
- Bundle:
  `/tmp/mini_vpn/knife14cd_pool2_ab_20260706_1616/mvpn_knife14cd_pool2_ab_usclient_suite_20260707_001631.tar.gz`
- Direct baselines remained healthy:
  - `.27 -> .77` reverse receiver: `278 Mbit/s`
  - `.33 -> .77` reverse receiver: `285 Mbit/s`
- Reverse-first P1 recovered from no-data to a low-average run:
  - sender: `27.0 Mbit/s`
  - receiver: `25.9 Mbit/s`
  - `throughput_shape=shape=low_average`
- TUIC stream starvation cleared:
  - `tcp_pool: opens=2 conns=0,1`
  - data stream `data_first_rx_max_ms=3`
  - data stream `data_rx_bytes_max=101247460`
- The remaining failure moved back to local egress/drop pressure:
  - `tun_tx_dropped_delta=541`
  - `send_queue_max=892928`
  - `pending_at_close=553066`
  - `egress_at_close=892928`
  - `close_pending_class=active_send_capable`
  - `hard_edge_guard_limited=1701` in final summaries

## Decision

Knife14cc is not accepted as the final fix. Its guard remains a useful local
egress protection, but the pool=1 reruns exposed a separate same-connection
TUIC TCP stream starvation branch.

Knife14cd should make pool=2 the default for product and suite acceptance while
preserving explicit pool=1 for constrained-server compatibility and A/B
diagnostics. After pool=2 is default, any remaining failure should be treated
as local egress/drop/backlog pressure, not as the same pool=1 no-data branch.
