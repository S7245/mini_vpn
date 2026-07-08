# 2026-07-08 Knife14fs Explicit TUIC Relay Stream Results

## Goal

Test whether the remaining reverse-first starvation was caused by wrapping
quinn's `RecvStream` and `SendStream` with `tokio::io::join`.

The stage implemented an explicit `TuicTcpRelayStream { recv, send }` and kept
the existing `TrackedRelayStream` diagnostics around it.

Acceptance target remained:

- clean reverse-first P1 receiver `100+ Mbit/s`;
- `pending_at_close=0`;
- `terminal_pending_reap=0`;
- `tun_tx_dropped_delta=0`;
- QUIC loss/blocking/congestion `0`;
- no persistent `connection_stream_frames_pending` on the active data stream.

## Code

- Commit: `f25c952` (`fix: use explicit TUIC TCP relay stream`)
- Local checks:
  - `cargo test tuic_tcp_relay_stream_uses_recv_for_reads_and_send_for_writes --lib`
  - `cargo test tracked_relay_stream_records_nonempty_reads --lib`
  - `cargo test tuic --lib`
  - `cargo test --lib`
  - `rustfmt --edition 2024 --check src/tuic.rs`
  - `cargo build --release`

`cargo fmt --check` still reports pre-existing formatting drift across unrelated
files, so this stage used edition-aware single-file rustfmt for the touched file.

## Setup

- Client: `.27`, clean clone at `/home/ubuntu/mini_vpn_accept`
- Code: `f25c952`
- Exit: `.33`, sing-box active
- Target: `.77`, iperf3 active
- Exit socket buffers:

```text
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
```

The focused run disabled target SSH evidence collection to avoid route/log
pollution and kept the exit-to-target iperf preflight enabled.

## Bundle

- Remote:
  `/tmp/conn/mvpn_knife14fs_explicit_tuic_stream_p1_30_usclient_suite_20260708_101623.tar.gz`
- Local:
  `/tmp/mini_vpn/knife14fs_explicit_tuic_stream_p1_30/mvpn_knife14fs_explicit_tuic_stream_p1_30_usclient_suite_20260708_101623.tar.gz`

## Preflight

Direct baselines were healthy:

- `.27 -> .77`: receiver about `280 Mbit/s`
- `.77 -> .27`: receiver about `279 Mbit/s`
- `.33 -> .77`: receiver about `261 Mbit/s`
- `.77 -> .33`: receiver about `282 Mbit/s`

The mini_vpn startup path used safe1200 correctly:

- `QUIC MTU policy=safe1200`
- `dg_max=Some(1166)`
- PLPMTUD probes/loss/black holes stayed `0`

## Result

The tunnel P1 exited normally but failed badly:

```text
sender   1.29 Mbit/s
receiver 0.132 Mbit/s
shape    no_data
```

The interval profile contained mostly zero-throughput seconds, with only small
bursts at a few points.

## Clean Local/QUIC Surfaces

The local downlink controller was not under pressure in the active window:

```text
pending_total_max=0
pending_max=0
may_recv_false=0
headroom_deferred_bytes=0
send_queue_max=3509
send_slice_zero=0
send_slice_errors=0
tun_flush_failures=0
```

Close and TUN surfaces stayed clean:

```text
pending_at_close=0
terminal_pending_reap=0
egress_at_close=0
tun_tx_dropped_delta=0
```

QUIC stayed clean:

```text
lost/congestion_events deltas = 0
tx_blocked(data,stream) deltas = 0
rx_blocked(data,stream) deltas = 0
```

## Failure Signal

The active data stream still starved while connection-level stream frames
arrived:

```text
tuic_stream_pending_causes: connection_stream_frames_pending=21
data_pending_gap_max_ms=17024
data_read_gap_max_ms=13776
data_poll_gap_max_ms=13748
data_rx_bytes_max=493680
```

Representative line:

```text
pending_cause=connection_stream_frames_pending conn_rx_stream_frames=434 conn_rx_stream_frames_since_read=47
```

The relay data flow saw only `493680` bytes, despite the direct path being
healthy. `ack_drain_hint` fired repeatedly, but there was no local queue pressure
to relieve.

## Interpretation

Knife14fs rejects the hypothesis that `tokio::io::join(recv, send)` is the
remaining root. Replacing it with an explicit `AsyncRead/AsyncWrite` wrapper did
not improve wakeup or delivery; throughput collapsed to a no-data shape while
the same `connection_stream_frames_pending` discriminator remained dominant.

This is stronger than the earlier local-credit evidence:

- local pending/headroom/may_recv pressure can be completely clean;
- QUIC loss, congestion, PLPMTUD, and flow-control blocking can be clean;
- the connection can report stream-frame progress;
- yet the active ordered stream read can remain pending for many seconds.

Therefore the next branch should not keep tuning:

- local downlink credit;
- egress pacer constants;
- TUN RX drain/ACK budgets;
- TCP pool size;
- MTU/PLPMTUD;
- `tokio::io::join` vs a simple wrapper.

## Next Plan

Do not continue code edits from `f25c952` without a new confirmed plan. The next
coherent step should either revert or isolate `f25c952`, then move below the
wrapper layer:

1. Add a protocol-level trace that correlates quinn per-stream read readiness,
   ordered stream offsets, and connection-level stream-frame counters.
2. Capture a same-window sing-box/server-side sending trace or qlog-equivalent
   for `.33 -> .27`, with no secrets in artifacts.
3. Run a minimal A/B where the target sender is bounded to a known low rate and
   compare whether `RecvStream` still reports multi-second pending while
   connection frames advance.
4. If quinn ordered-stream HOL is confirmed, evaluate a controlled custom-exit
   architecture or TUIC data-channel variant rather than further mini_vpn local
   credit tuning.
