# 2026-07-09 Knife14h4 Ordered Chunk Results

## Goal

Try H4: replace the default ordered TUIC TCP read adapter with a product
ordered chunk adapter that uses Quinn `read_chunk(max, true)` rather than
`tokio::io::join(recv, send)` on the read side.

This stage deliberately did not tune VPS services, iperf3, MTU/PLPMTUD, stale
TUIC pool handling, or broad QUIC windows.

## Code State

Experimental local diff, not yet accepted as a throughput fix:

- `src/tuic.rs`

Main code changes:

- Added `OrderedChunkRecv` and a Quinn-backed `QuinnOrderedChunkRecv`.
- Added `TuicOrderedRelayStream`, preserving ordered TCP semantics while
  delegating writes to the original QUIC send half.
- Changed the ordered TUIC open path from `tokio::io::join(recv, send)` to
  `TuicOrderedRelayStream::from_quinn(recv, send)`.
- Changed the open diagnostic label from `ordered_join` to `ordered_chunk` so
  VPS bundles prove the new path was active.

Focused TDD:

- `ordered_relay_stream_reads_ordered_chunks_without_join`
- `ordered_relay_stream_keeps_remainder_without_repolling_recv`
- `ordered_relay_stream_delegates_writes_to_quic_send_half`

Local gates passed:

- `cargo fmt --check`
- `cargo test -q ordered_relay_stream_`
- `cargo test -q format_tuic_tcp_open_line_includes_target_pool_and_id`
- `cargo test -q tuic::tests::`
- `cargo check -q`
- `cargo test -q`
- `cargo clippy -q --all-targets --all-features`

Remote `.27` focused gates passed in a temporary clean archive at
`/tmp/mini_vpn_h4`:

- `cargo test -q tuic::tests::`
- `cargo check -q`
- `cargo build --release` via the suite

The temporary archive intentionally avoided overwriting the dirty
`/home/ubuntu/mini_vpn` worktree.

## VPS Acceptance

Suite:

- tag: `knife14h4_ordered_chunk_p1`
- remote bundle:
  `/tmp/conn/mvpn_knife14h4_ordered_chunk_p1_usclient_suite_20260709_135541.tar.gz`
- local bundle:
  `/tmp/mini_vpn_knife14h4_ordered_chunk/mvpn_knife14h4_ordered_chunk_p1_usclient_suite_20260709_135541.tar.gz`
- report:
  `/tmp/conn/mvpn_knife14h4_ordered_chunk_p1_usclient_suite_20260709_135541.md`
- tunnel report:
  `/tmp/conn/mvpn_knife14h4_ordered_chunk_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_135541.md`

Preflight:

- `.33` sing-box active.
- `.77` iperf3 active.
- `.33` socket buffers remained high:
  `rmem_max=16777216`, `wmem_max=16777216`,
  `rmem_default=1048576`, `wmem_default=1048576`.
- Direct baseline remained healthy:
  `.27 -> .77` receiver `275 Mbit/s`,
  `.77 -> .27` receiver `277 Mbit/s`.
- Runtime logs proved the H4 path was active:
  `relay_mode=ordered_chunk`.

Reverse-first P1 result:

| Metric | Value |
| --- | ---: |
| iperf sender | `17.100 Mbit/s` |
| iperf receiver | `15.700 Mbit/s` |

The first threshold failed. H4 did not exceed `30 Mbit/s` and did not approach
the final `100+ Mbit/s` target.

## Discriminators

Clean signals:

- `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=0`
- `global_rx_pressure events=0`, `queue_used_max=106/1024`
- `local_write_pressure events=0`
- `pending_at_close=0`, `terminal_pending_reap=0`
- QUIC loss/congestion/blocking deltas `0`
- `downlink_flush accepted_bytes=59055887`
- `send_capacity_min=max=1048576`
- `headroom_limited=0`, `pressure_credit_debt_bytes=0`

Remaining bad signals:

- Iperf remained bursty/idle, with many `0.00 bits/sec` intervals.
- `data_read_gap_max_ms=3685`
- `data_pending_gap_max_ms=3006`
- `data_poll_gap_max_ms=405`
- fresh/stale pending still appeared:
  `connection_fresh_stream_frames_pending=9`,
  `connection_stale_stream_frames_pending=10`,
  `connection_rx_no_stream_frames=2`
- data stream reached only `59055887` bytes over the 30s reverse window.

Compared with Knife14hz, H4 reduced the worst data read gap from about
`5086ms` to `3685ms`, but that improvement was not throughput-significant.

## Conclusion

H4 is a real architectural slice, not a parameter tweak, but it is not a
sufficient fix. Replacing `tokio::io::join(...).read()` with ordered
`read_chunk(true)` did not move the system out of the `15-20 Mbit/s` band.

Do not continue the same branch by only adjusting ordered chunk size, self-wake
intervals, or relay-read batching. The next design must explain why remote data
delivery remains bursty while QUIC receive, local admission, TUN drops,
global-rx pressure, and close-tail counters are clean.

Before another VPS run, decide whether to revert H4, keep it gated as an
opt-in diagnostic path, or keep it only if the next design can use it as a
necessary seam. It should not be claimed as the `100+ Mbit/s` path by itself.
