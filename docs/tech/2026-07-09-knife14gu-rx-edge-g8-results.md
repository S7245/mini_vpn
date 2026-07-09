# 2026-07-09 Knife14gu RX Edge G8 Results

## Goal

Run the first cleanup slice after Knife14gt G7 reached `147/144 Mbit/s` but
left a dirty close tail.

The G8 hypothesis was deliberately narrow:

- preserve the G7 `128KiB` relay dispatch / local egress cadence;
- stop extra relay ready-burst reads when the relay-to-main `global_rx` queue
  is at the critical edge;
- keep safe1200, ordered TUIC TCP, and the existing VPS/sing-box setup;
- do not touch iperf3, MTU/PLPMTUD, stale pool, or broad QUIC windows.

The intended clean gate was:

- receiver remains `>100 Mbit/s`;
- `tx_dropped_delta=0`;
- `terminal_pending_reap_bytes=0`;
- no data-relay `terminal_closed_no_send`;
- no global RX queue edge saturation.

## Code State

- Commit: `1a3c5cb`
  (`fix(knife14gu): stop relay ready bursts at rx edge`)
- Branch: `codex/knife14d-downlink-reap-open`
- Remote workdir on `.27`: `/tmp/mini_vpn_knife14gu_1a3c5cb`
- The dirty `/home/ubuntu/mini_vpn` worktree on `.27` was not modified.
- The remote workdir was an rsync clean copy without `.git`, so the suite's
  `git rev-parse` check reported `not a git repository`. The code point is the
  pushed local commit above.

Local gates executed:

- TDD red:
  `cargo test relay_remote_ready_burst_stops_extra_reads_at_global_rx_critical_edge -- --nocapture`
- TDD green:
  `cargo test relay_remote_ready_burst_stops_extra_reads_at_global_rx_critical_edge -- --nocapture`
- `cargo test relay_remote_ready_burst_tapers_when_global_rx_queue_is_busy -- --nocapture`
- `cargo test relay_ready_batch_limit_tapers_with_global_rx_queue_pressure -- --nocapture`
- `cargo test relay_remote_ready_burst -- --nocapture`
- `cargo test --features harness --test concurrency_harness -- --nocapture`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- `cargo build --release`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `git diff --check`

Remote focused gates:

- `cargo test relay_remote_ready_burst_stops_extra_reads_at_global_rx_critical_edge -- --nocapture`
- `cargo test relay_ready_batch_limit_tapers_with_global_rx_queue_pressure -- --nocapture`
- `cargo build --release`

## Artifacts

Remote bundle:

- `/tmp/knife14gu_g8_1a3c5cb/mvpn_knife14gu_rxedge_safe1200_p1_usclient_suite_20260709_092357.tar.gz`

Local pulled bundle:

- `/tmp/mini_vpn_knife14gu_g8/mvpn_knife14gu_rxedge_safe1200_p1_usclient_suite_20260709_092357.tar.gz`

Pulled files:

- `/tmp/mini_vpn_knife14gu_g8/mvpn_knife14gu_rxedge_safe1200_p1_usclient_suite_20260709_092357.md`
- `/tmp/mini_vpn_knife14gu_g8/mvpn_accept_20260709_092357.log`
- `/tmp/mini_vpn_knife14gu_g8/mvpn_knife14gu_rxedge_safe1200_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_092357.md`

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
| `.27 -> .77` | `278 Mbit/s` |
| `.77 -> .27` | `277 Mbit/s` |

Startup confirmed:

- buffered downlink enabled;
- safe1200 TUIC MTU policy;
- QUIC UDP socket buffers at `16777216B`;
- TUN MTU `1200`, qlen `500`.

## VPS Result

Reverse-first P1:

| Metric | Value |
| --- | ---: |
| iperf sender | `20.2 Mbit/s` |
| iperf receiver | `19.2 Mbit/s` |

Stage answer: **G8 failed the clean `100+ Mbit/s` gate and also regressed below
the earlier `>30 Mbit/s` discriminator.**

The interval shape returned to burst/idle: some one-second windows reached
`88.2 Mbit/s` and `153 Mbit/s`, but many windows were `0.00`, leaving the
30-second receiver average at `19.2 Mbit/s`.

## Key Signals

Signals that improved versus the dirty G7 tail:

- `tx_dropped_total=0`, `tx_dropped_delta=0`.
- No `tcp-tun-egress-feedback paused=true` line appeared in the parsed run.
- `global_rx_queue_used_max` on the data relay reached only `210/1024`,
  instead of G7's `1019/1024` edge.
- `global_rx_pressure_events=0`, `global_rx_wait_max_us=667`.
- The control handle closed cleanly with `terminal_pending_reap_bytes=0`.
- Data-path pending remained bounded in the visible metrics:
  `pending_total=0`, `pending_high=131072`, `dirty_handles=0` during the main
  run.
- `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_tx_failures=0`.

Signals that explain the failure:

- The new critical-edge limiter did not activate in the real run:
  `remote_batch_limited=0` and `remote_batch_limit_bytes_min=524288`.
- The data relay stayed far from the critical threshold:
  `global_rx_queue_used_max=210`, while the new guard only stops extra reads at
  the last `8` free slots.
- Ordered TUIC stream delivery again had multi-second gaps:
  `max_remote_read_gap_ms=1709`, then `2107`, then `3406`, then `3610`.
- The run repeatedly logged `tuic-tcp-stream-pending` with
  `pending_cause=connection_stream_frames_pending`.
- Some pending windows still had fresh transport stream-frame evidence since
  pending, for example `conn_rx_stream_frames_since_pending=6397`, `9335`,
  `19970`, and `6166`.
- Later repeated pending windows also had stale evidence again
  (`conn_rx_stream_frames_since_pending=0`), so both fresh-frame and stale-frame
  pending shapes remain possible.
- `tcp-local-egress-service` still did not become the useful data-admission
  lane: the final line showed `accepted_bytes=0`,
  `no_progress=723`, `no_work=4273`.

## Interpretation

The G8 code change is a valid local safety guard, but it is not a sufficient
throughput fix.

The important discriminator is that the new guard did not actually fire during
the low-throughput run. Therefore the G8 regression cannot be explained as
"the near-full guard throttled the relay reader." The run instead returned to
the older ordered-stream burst/idle shape: multi-second TUIC stream read gaps,
connection-level stream-frame evidence during some pending windows, and no
useful progress from the local egress service lane.

This means the G7 dirty tail cannot be reduced to one root cause of
`global_rx` near-full pressure. G7 proved that the current branch can move
`100+ Mbit/s`; G8 proved that a tail-edge queue guard alone does not preserve
that cadence.

## Stop Condition

Per the project rule for failed repair tests, no further code change should be
started from this result alone.

Before another implementation slice, do an A/B repeat to separate run variance
from a real commit regression:

1. Repeat G8 once at `1a3c5cb` with the same safe1200 reverse-first P1 shape.
2. If it stays low, rerun the parent high-throughput commit `4caf60a` in the
   same clean workdir/suite shape.
3. Only after the A/B result, choose the next code direction.

If the A/B confirms that `4caf60a` still reaches `100+` while `1a3c5cb` stays
low, inspect the guard's indirect scheduling effects despite
`remote_batch_limited=0`. If both are low, treat G7 as a high-throughput but
not yet reproducible run and refocus on the TUIC ordered stream service path:
pending/read readiness, connection stream-frame accounting, and local
admission feedback in one measured contract.
