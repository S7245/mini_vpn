# 2026-07-08 Knife14fq Timeout120 Clean-Tail Regression Results

## Goal

Test whether the dirty close-tail left by Knife14fp was primarily caused by the
probe's external iperf timeout. The intended final 1% acceptance was:

- keep the exit-side socket-buffer preflight from Knife14fp;
- run the same safe1200 reverse-first P1 with a longer iperf timeout;
- preserve `100+ Mbit/s` receiver throughput;
- clean `pending_at_close`, `terminal_pending_reap`, `tun_tx_dropped_delta`,
  and QUIC loss/blocking.

## Setup

- Client: `.27`, clean clone at `/home/ubuntu/mini_vpn_accept`
- Code: `f8765c1`
- Exit: `.33`, sing-box active
- Target: `.77`, iperf3 active
- Exit socket buffers:

```text
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
```

The clean clone source hashes matched the existing `.27` working tree for the
key Rust and suite files, so the regression is not explained by using different
source content.

## Bundle

- Remote:
  `/tmp/conn/mvpn_knife14fq_socketbuf_timeout120_p1_30_usclient_suite_20260708_094207.tar.gz`
- Local:
  `/tmp/mini_vpn/knife14fq_socketbuf_timeout120_p1_30/mvpn_knife14fq_socketbuf_timeout120_p1_30_usclient_suite_20260708_094207.tar.gz`

## Result

The run exited normally instead of being killed by timeout:

```text
timeout 120s iperf3 -c 43.130.32.77 -p 5201 -t 30 -P 1 -R
exit=0
```

Throughput regressed:

```text
sender   37.0 Mbit/s
receiver 35.7 Mbit/s
shape    low_average
```

The interval profile was burst/idle, with many zero-second intervals and
`tail_avg_mbps=17.850`.

## Clean Signals

- `pending_at_close=0`
- `terminal_pending_reap=0`
- `egress_at_close=0`
- `tun_tx_dropped_delta=0`
- QUIC loss/congestion/blocking deltas `0`
- `max_rx_blocked_stream_delta=0`
- no `send_slice` zero/errors
- no TUN flush failures

## Dirty Signals

The local pressure edge returned:

```text
downlink_backpressure pause_edges=1 resume_edges=0
max_pending_bytes=10524
max_tx_queue_bytes=503692
may_recv_false=13594
headroom_limited=13520
headroom_deferred_bytes=17512861
pressure_credit_blocked_bytes=923353
hard_edge_guard_limited=1
hard_edge_guard_deferred_bytes=10524
```

The data stream received only `134282739` bytes in the window, compared with
`713311282` bytes in the Knife14fp high-throughput timeout run. The data stream
also showed multi-second pending/read gaps up to about `4235ms`.

## Comparison With Knife14fp

Knife14fp:

- `IPERF_TIMEOUT_SECS=50`
- iperf ended with `exit=124`
- reported receiver `114.000 Mbit/s`
- first 29 non-zero intervals averaged `189.483 Mbit/s`
- close-tail was dirty:
  `terminal_pending_reap=349932`,
  `pending_at_close=349932`,
  `egress_at_close=449999`,
  `tun_tx_dropped_delta=16`

Knife14fq:

- `IPERF_TIMEOUT_SECS=120`
- iperf ended with `exit=0`
- receiver `35.700 Mbit/s`
- close-tail was clean
- local pressure/headroom gating returned during the data window

## Interpretation

Longer timeout fixes the timeout-driven close-tail symptom, but it is not a
final throughput fix. Exit-side socket buffers remain mandatory and the
mature-client A/B remains decisive for the server-side prerequisite, but
mini_vpn has not yet earned clean final `100+ Mbit/s` acceptance.

The current root is more specific than the earlier broad branches: with the
exit socket-buffer preflight satisfied, low repeats can still hit local
pressure/headroom gating even when QUIC loss/blocking and TUN drops are clean.

## Next Plan

Do not random-edit Rust after this failed acceptance. The next stage should:

1. Repeat a minimal A/B on the same clean binary, controlling only
   `IPERF_TIMEOUT_SECS` and post-iperf settle, and skipping target SSH evidence
   where possible to avoid post-probe route pollution.
2. If the low result repeats, implement a small local pressure-edge fix around
   the case where `pending` is small, `send_queue` is near the flush/credit
   edge, TUN drops are zero, and `may_recv_false/headroom_deferred` dominate.
3. Keep the production VPS socket-buffer guide as mandatory, but require a
   clean normal-exit `100+ Mbit/s` repeat before declaring Knife14 complete.
