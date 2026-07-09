# Knife14gw Stream-Service Diagnostics Results

Date: 2026-07-09

## Scope

This stage implemented the code-review target set 1-5:

- TUIC stream pending freshness classification.
- `StreamServiceWindow` / `StreamServiceController` diagnostics.
- relay reader service controller seam for awaited read length.
- runtime stream-service window logs carrying remote read, global-rx, and local admission evidence.
- one focused safe1200 reverse-first P1 VPS gate.

Non-goals: no VPS tuning, no iperf3 changes, no MTU/PLPMTUD work, no stale-pool work, and no broad QUIC window changes.

## Code State

- Code commit: `946e2a8` (`feat(knife14): add stream service diagnostics`)
- Branch: `codex/knife14d-downlink-reap-open`
- Remote workdir on `.27`: `/tmp/mini_vpn_knife14gw_946e2a8`
- The dirty `/home/ubuntu/mini_vpn` worktree on `.27` was not modified.

Local gates:

- TDD red for `tuic_tcp_stream_pending_cause_classifies_transport_freshness` before adding fresh/stale variants.
- `cargo test tuic_tcp_stream_pending_cause_classifies_transport_freshness --lib`
- `cargo test stream_service --lib`
- `cargo test relay_remote_ready_burst --lib`
- `cargo test relay_read_credit --lib`
- `cargo test relay_ready_burst_delivers_data_before_eof_close --lib`
- `cargo test tuic_tcp_stream --lib`
- `cargo test --lib --quiet`
- `cargo test --quiet`
- `cargo fmt`
- `git diff --check`

Remote focused gates on `.27`:

- `cargo test stream_service --lib`
- `cargo test tuic_tcp_stream_pending_cause_classifies_transport_freshness --lib`
- `cargo build --release`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`

## Artifacts

Remote bundle:

- `/tmp/knife14gw_stream_service_946e2a8/mvpn_knife14gw_stream_service_946e2a8_usclient_suite_20260709_102512.tar.gz`

Local pulled bundle:

- `/tmp/mini_vpn_knife14gw_stream_service_946e2a8/mvpn_knife14gw_stream_service_946e2a8_usclient_suite_20260709_102512.tar.gz`

Pulled files:

- `/tmp/mini_vpn_knife14gw_stream_service_946e2a8/mvpn_knife14gw_stream_service_946e2a8_usclient_suite_20260709_102512.md`
- `/tmp/mini_vpn_knife14gw_stream_service_946e2a8/mvpn_accept_20260709_102512.log`
- `/tmp/mini_vpn_knife14gw_stream_service_946e2a8/mvpn_knife14gw_stream_service_946e2a8_usclient_tunnel_mtu1200_reverse_first_p1_20260709_102512.md`
- `/tmp/mini_vpn_knife14gw_stream_service_946e2a8/mvpn_knife14gw_stream_service_946e2a8_server_evidence_mtu1200_reverse_first_p1_20260709_102512.md`

## Preflight

Read-only service preflight before the suite:

- `.33` sing-box: `active`
- `.33` socket buffers:
  - `net.core.rmem_max = 16777216`
  - `net.core.wmem_max = 16777216`
  - `net.core.rmem_default = 1048576`
  - `net.core.wmem_default = 1048576`
- `.77` iperf3: `active`

Suite direct path was healthy:

| Path | Receiver |
| --- | ---: |
| `.27 -> .77` | `275 Mbit/s` |
| `.77 -> .27` | `278 Mbit/s` |

## VPS Result

Focused safe1200 reverse-first P1 did not pass the `30 Mbit/s` discriminator:

| Direction | Sender | Receiver |
| --- | ---: | ---: |
| reverse-first P1 | `17.7 Mbit/s` | `17.1 Mbit/s` |

The interval profile was bursty rather than stable: several 1s buckets were `0.00 bits/sec`, while the final bucket reached `99.6 Mbit/s`.

## Key Signals

The low result was not explained by the already rejected external/config branches:

- direct baselines were healthy;
- `.33` socket buffers stayed at the known high-throughput values;
- `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=0`;
- QUIC loss/congestion/blocking deltas were `0`;
- `global_rx_pressure_events=0`, with queue high `126/1024`;
- local write pressure events were `0`;
- `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_tx_failures=0`;
- pending-at-close and terminal-pending reap were clean (`0` bytes).

The dominant data-stream shape remains ordered TUIC stream cadence:

- data relay `remote_to_global_rx_bytes=64401389`;
- `remote_read_service_len_min=65536`, `remote_read_service_len_max=65536`;
- `remote_batch_bytes_max=131072`;
- `max_remote_read_gap_ms=3464` on the data stream;
- `tuic_stream_pending` data max gap `3464ms`;
- `self_wake_armed=10658`, `self_wake_fired=8002`;
- close had `close_egress_class=terminal_closed_no_send` with `close_egress_bytes=5632`.

The new raw diagnostics worked:

- raw `tuic-tcp-stream-pending` lines showed both `connection_fresh_stream_frames_pending` and `connection_stale_stream_frames_pending`;
- raw `tcp-stream-service-window` lines showed cumulative remote service and local-admission progress. The final data handle window reached `remote_poll_ticks=8250`, `remote_read_chunks=8600`, `remote_read_bytes=64401389`, `global_rx_queue_used_max=126`, and `useful_progress=true`.

Parser caveat: the suite attribution summary has not yet been updated for the new fresh/stale pending-cause names, so its `tuic_stream_pending_causes` line still reports only legacy fields. Use the raw `tuic-tcp-stream-pending` lines for Knife14gw pending freshness interpretation.

## Conclusion

Knife14gw completed the diagnostics/framework slice, but it did not move throughput above `30 Mbit/s`. The new evidence narrows the next implementation target: the problem is still not VPS config, MTU, stale pool, global-rx pressure, TUN drops, or local send capacity. The next code change should target the ordered TUIC stream-service cadence itself and keep the new stream-service window as the acceptance lens.
