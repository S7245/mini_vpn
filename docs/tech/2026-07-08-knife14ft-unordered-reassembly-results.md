# 2026-07-08 Knife14ft Unordered TUIC Reassembly Results

## Goal

Test whether the remaining reverse-first starvation was caused by quinn ordered
stream head-of-line behavior that could be relieved by reading unordered QUIC
chunks and reassembling them locally by stream offset before exposing bytes to
the TCP relay.

Acceptance target remained:

- clean reverse-first P1 receiver `100+ Mbit/s`;
- `pending_at_close=0`;
- `terminal_pending_reap=0`;
- `tun_tx_dropped_delta=0`;
- QUIC loss/blocking/congestion `0`;
- no persistent active-data `connection_stream_frames_pending`.

## Code

- `8b1d86d` reverted the rejected explicit stream wrapper from Knife14fs.
- `a4bfbe6` added `TuicChunkRelayStream`, which calls
  `RecvStream::read_chunk(max, ordered=false)`, buffers chunks in a bounded
  offset reassembler, and only returns contiguous ordered bytes to the existing
  relay pump.
- New diagnostics emit `tuic-tcp-unordered-staging` when out-of-order chunks or
  the local staging cap are observed.

Local checks:

- `cargo test ordered_quic_chunk_assembler --lib`
- `cargo test unordered --lib`
- `cargo test tuic --lib`
- `cargo test --lib`
- `rustfmt --edition 2024 --check src/tuic.rs`
- `cargo build --release`

`cargo fmt --check` still reports pre-existing formatting drift across unrelated
files, so this stage used edition-aware single-file rustfmt for the touched file.

## Bundle

- Remote:
  `/tmp/conn/mvpn_knife14ft_unordered_reassembly_p1_30_usclient_suite_20260708_103800.tar.gz`
- Local:
  `/tmp/mini_vpn/knife14ft_unordered_reassembly_p1_30/mvpn_knife14ft_unordered_reassembly_p1_30_usclient_suite_20260708_103800.tar.gz`

## Setup

- Client: `.27`, clean clone at `/home/ubuntu/mini_vpn_accept`
- Code: `a4bfbe6`
- Exit: `.33`, sing-box active
- Target: `.77`, iperf3 active
- QUIC MTU policy: `safe1200`
- TCP diagnostics: enabled

Preflight direct baselines were healthy:

- `.27 -> .77`: receiver about `277 Mbit/s`
- `.77 -> .27`: receiver about `279 Mbit/s`
- `.33 -> .77`: receiver about `281 Mbit/s`
- `.77 -> .33`: receiver about `288 Mbit/s`

Startup confirmed:

- `QUIC MTU policy=safe1200`
- `dg_max=Some(1166)`
- PLPMTUD probes/loss/black holes stayed `0`
- `.33` high socket-buffer setting was still present from Knife14fp

## Result

The focused reverse-first P1 failed badly:

```text
sender   0.280 Mbit/s
receiver 0.004 Mbit/s
shape    no_data
```

Only one interval delivered meaningful payload:

```text
10.00-11.00 sec  13.8 KBytes  113 Kbits/sec
```

## Clean Local/QUIC Surfaces

Local pressure was clean:

```text
pending_total_max=0
pending_max=0
may_recv_false=0
headroom_deferred_bytes=0
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

QUIC path/blocking counters stayed clean:

```text
max_lost_bytes_delta=0
max_congestion_events_delta=0
max_tx_blocked_data_delta=0
max_tx_blocked_stream_delta=0
max_rx_blocked_data_delta=0
max_rx_blocked_stream_delta=0
```

## New Discriminator

The new unordered staging log proved that quinn did expose out-of-order stream
chunks:

```text
tuic-tcp-unordered-staging reason=chunk next_offset=0 chunk_offset=14102 chunk_bytes=18 gap_bytes=14102 buffered=18B
tuic-tcp-unordered-staging reason=chunk next_offset=14120 chunk_offset=43772 chunk_bytes=1408 gap_bytes=29652 buffered=5728B
tuic-tcp-unordered-staging reason=chunk next_offset=14120 chunk_offset=62092 chunk_bytes=36 gap_bytes=47972 buffered=29702B
```

The local staging cap was not hit:

```text
cap_hits=0
cap=4194304B
max_buffered=29702B
```

The active stream then stayed pending while connection-level stream frames
advanced:

```text
tuic_stream_pending_causes: connection_stream_frames_pending=34
data_pending_gap_max_ms=23033
data_read_gap_max_ms=23935
data_rx_bytes_max=66352
conn_rx_stream_frames_since_read=35
```

The relay summary matched that starvation:

```text
remote_to_global_rx_bytes=66352
remote_batches=4
remote_batch_bytes_max=48008
remote_batch_chunks_max=10
ack_drain_hint_sent=25
```

## Interpretation

Knife14ft rejects default unordered chunk reassembly as a throughput fix. The
diagnostic is useful because it shows real TUIC/QUIC stream offset gaps, but
moving out-of-order chunks into mini_vpn-local staging did not convert them into
steady ordered TCP payload. It made the P1 no-data shape worse than the prior
known-good high-buffer mini_vpn run.

The remaining failure is not local downlink pending/headroom, TUN egress drops,
QUIC congestion/loss/blocking, PLPMTUD, pool staleness, or join-vs-wrapper. It
is a deeper ordered stream delivery/sender cadence problem on the TUIC TCP data
stream: bytes arrive at later offsets, the required lower offsets are delayed
for many seconds, and the target sender then stalls.

## Required Next Step

Do not keep `a4bfbe6` as the default data path. Before the next acceptance run,
choose one of these cleanup paths:

1. Revert `a4bfbe6` completely, returning to the last accepted ordered stream
   path.
2. Keep the unordered reassembler only behind an explicit diagnostic env flag
   such as `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1`, with the default path
   restored to ordered `tokio::io::join(recv, send)`.

The next real repair branch should target one of:

- a server-side sending cadence / qlog-style trace for `.33 -> .27`;
- a quinn per-stream offset readiness trace that records missing ordered ranges
  without changing default data flow;
- a custom-exit or alternate TUIC data-channel discriminator if sing-box TUIC
  single-stream delivery remains offset-gap limited under high throughput.
