# 2026-07-09 Knife14gy Split Poll Credit Results

## Goal

Test the next focused fix for the current Knife14 data-plane bottleneck:

- ordered TUIC stream polling must not be capped by small local
  pressure-credit/headroom feedback;
- hard pause/zero credit must still stop remote reads;
- reverse-first P1 must show whether this restores movement beyond the first
  `30 Mbit/s` threshold.

This stage deliberately did not change VPS service config, iperf3, MTU/PLPMTUD,
stale pool handling, or broad QUIC window settings.

## Code State

Code commit:

- `372d6e3` (`fix(knife14): decouple stream polling from local credit`)

Main code change:

- Added a stream-poll credit projection that keeps ordered TUIC stream read
  service at least one dispatch segment when the flow is not hard-paused.
- Kept hard paused/zero credit behavior intact.
- Applied the projected credit to relay read probes and the ordered relay
  reader paths before `next_remote_read_len(...)`.

Focused TDD gate:

- `local_pressure_credit_does_not_cap_ordered_stream_poll_below_dispatch_window`
  first failed on the old behavior (`19200` byte read len versus expected
  dispatch segment), then passed after the fix.

Local gates passed:

- `cargo test local_pressure_credit_does_not_cap_ordered_stream_poll_below_dispatch_window --lib -- --nocapture`
- `cargo test relay_read_credit_pause_stops_remote_reads_until_resumed --lib -- --nocapture`
- `cargo test relay_ack_drain_hint_polling_includes_low_byte_active_read_service --lib -- --nocapture`
- `cargo test relay_read_credit --lib -- --nocapture`
- `cargo test relay_ack_drain_hint --lib -- --nocapture`
- `cargo test stream_service --lib -- --nocapture`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `git diff --check`

Remote `.27` focused gates passed in a clean worktree:

- `cargo test local_pressure_credit_does_not_cap_ordered_stream_poll_below_dispatch_window --lib -- --nocapture`
- `cargo test relay_read_credit_pause_stops_remote_reads_until_resumed --lib -- --nocapture`
- `cargo test --lib`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `git diff --check`
- `cargo build --release`

## VPS Acceptance

Suite:

- tag: `knife14gy_splitpoll_safe1200_p1`
- remote bundle:
  `/tmp/conn/mvpn_knife14gy_splitpoll_safe1200_p1_usclient_suite_20260709_115932.tar.gz`
- local bundle:
  `/tmp/mini_vpn_knife14gy_splitpoll/mvpn_knife14gy_splitpoll_safe1200_p1_usclient_suite_20260709_115932.tar.gz`
- report:
  `/tmp/conn/mvpn_knife14gy_splitpoll_safe1200_p1_usclient_suite_20260709_115932.md`
- tunnel report:
  `/tmp/conn/mvpn_knife14gy_splitpoll_safe1200_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_115932.md`

Environment checks:

- `.33` sing-box active.
- `.77` iperf3 active.
- `.33` socket buffers still high:
  `rmem_max=16777216`, `wmem_max=16777216`,
  `rmem_default=1048576`, `wmem_default=1048576`.
- Direct baseline remained healthy:
  `.27 -> .77` receiver `298 Mbit/s`,
  `.77 -> .27` receiver `278 Mbit/s`.

Reverse-first P1 result:

| Metric | Value |
| --- | ---: |
| iperf sender | `24.100 Mbit/s` |
| iperf receiver | `22.700 Mbit/s` |

This did not pass the `30 Mbit/s` first threshold and is far below the final
`100+ Mbit/s` target.

## Key Signals

The split-poll fix worked at the code-signal layer:

- data stream `remote_read_service_len_min=65536`
- data stream `remote_read_service_len_max=65536`
- data stream `remote_batch_bytes_max=131072`
- data stream `read_credit_limit_bytes_min=6686`
- data stream `read_credit_pause_updates=0`
- data stream `data_poll_gap_max_ms=3`

This means small local pressure-credit values no longer cap the ordered stream
poll length below the dispatch segment while the flow is not paused.

The throughput failure moved to the local admission/headroom layer:

- `throughput_shape=low_average`
- `attribution=local_pressure_credit`
- `may_recv_false=5190`
- `budget_limited=5179`
- `headroom_limited=5185`
- `headroom_deferred_bytes=6376329`
- `hard_edge_guard_limited=5185`
- `hard_edge_guard_deferred_bytes=6325030`
- `pressure_credit_debt_bytes=122727`
- `pressure_credit_blocked_bytes=77688`
- `pending_total_max=100780`
- `send_queue_max=557386`
- `tcp-local-egress-service accepted_bytes=0`

Clean or rejected surfaces:

- `tun_tx_dropped_delta=0`
- `global_rx_pressure=0`
- `global_rx_queue_used_max=67/1024`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `terminal_pending_reap=0`
- `pending_at_close=0`
- QUIC loss/congestion/blocking deltas were `0`

Ordered stream read completions still had multi-second no-data gaps even though
polling itself was frequent:

- `data_read_gap_max_ms=3428`
- `data_pending_gap_max_ms=3007`
- `connection_fresh_stream_frames_pending=6`
- `connection_stale_stream_frames_pending=9`

## Conclusion

Knife14gy is a successful framework correction but a failed throughput gate.
It proves that the ordered TUIC stream polling length is no longer directly
clamped by low local pressure-credit/headroom feedback. It does not restore the
data plane beyond `30 Mbit/s`.

The next code slice should not repeat split-poll, relay staging, VPS tuning,
MTU/PLPMTUD, stale pool, or broad QUIC window work. The remaining target is the
local admission/headroom feedback contract after remote bytes are read: the
flow must keep smoltcp/TUN egress making measurable progress instead of
installing pressure debt and stopping useful admission while the QUIC/TUIC path
is clean.
