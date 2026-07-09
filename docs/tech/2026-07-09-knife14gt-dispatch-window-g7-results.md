# 2026-07-09 Knife14gt Dispatch Window G7 Results

## Goal

Run the next focused VPS acceptance after Knife14gs G6 failed at
`28.6/27.6 Mbit/s`.

This slice deliberately kept the scope small:

- keep ordered-default TUIC TCP delivery;
- keep safe1200 and the existing VPS/sing-box configuration;
- do not touch iperf3, MTU/PLPMTUD, stale pool, or broad QUIC windows;
- test whether the current branch can move past the first `>30 Mbit/s` gate.

The code change aligned the relay reader dispatch segment with one local egress
service target window:

```text
RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES =
    LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW
```

That moved the dispatch segment from `64KiB` to `128KiB`, while still staying
below the downlink flush cap. The intent was to feed the main-loop payload path
with at least one local egress service window per remote dispatch event.

## Code State

- Commit: `4caf60a` (`fix(knife14gt): align relay dispatch with egress window`)
- Branch: `codex/knife14d-downlink-reap-open`
- Remote workdir on `.27`: `/tmp/mini_vpn_knife14gt_4caf60a`
- The dirty `/home/ubuntu/mini_vpn` worktree on `.27` was not modified.
- The remote workdir was an rsync clean copy without `.git`, so the suite's
  `git rev-parse` check reported `not a git repository`. The code point is the
  pushed local commit above.

Local gates executed:

- TDD red: the new dispatch-window guard failed before the constant change.
- `cargo test relay_dispatch_segment_covers_one_local_egress_service_window -- --nocapture`
- `cargo test relay_read_service_diag_records_awaited_read_window_range -- --nocapture`
- `cargo test`
- `cargo test --features harness --test concurrency_harness -- --nocapture`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo build --release`
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `git diff --check`

Remote focused gates:

- `cargo test relay_dispatch_segment_covers_one_local_egress_service_window -- --nocapture`
- `cargo build --release`

## Artifacts

Remote bundle:

- `/tmp/knife14gt_g7_4caf60a/mvpn_knife14gt_dispatch128_safe1200_p1_usclient_suite_20260709_085329.tar.gz`

Local pulled bundle:

- `/tmp/mini_vpn_knife14gt_g7/mvpn_knife14gt_dispatch128_safe1200_p1_usclient_suite_20260709_085329.tar.gz`

Pulled files:

- `/tmp/mini_vpn_knife14gt_g7/mvpn_knife14gt_dispatch128_safe1200_p1_usclient_suite_20260709_085329.md`
- `/tmp/mini_vpn_knife14gt_g7/mvpn_accept_20260709_085329.log`
- `/tmp/mini_vpn_knife14gt_g7/mvpn_knife14gt_dispatch128_safe1200_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_085329.md`

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
| `.27 -> .77` | `276 Mbit/s` |
| `.77 -> .27` | `274 Mbit/s` |

Startup confirmed:

- buffered downlink enabled;
- safe1200 TUIC MTU policy;
- QUIC UDP socket buffers at `16777216B`;
- TUN MTU `1200`, qlen `500`.

## VPS Result

Reverse-first P1:

| Metric | Value |
| --- | ---: |
| iperf sender | `147 Mbit/s` |
| iperf receiver | `144 Mbit/s` |

Stage answer: **G7 passed the `>30 Mbit/s` gate and exceeded `100 Mbit/s`.**

This is the first focused current-branch result after the G6 series showing
that mini_vpn can move data in the `100+ Mbit/s` band on the current
`.27/.33/.77` topology.

It is not final clean acceptance yet. The last six seconds had a tail wobble:
`36.7 Mbit/s`, `0`, `0`, `106 Mbit/s`, `0`, `0`. The average still stayed high,
but the close-tail counters were not clean.

## Key Signals

Improved throughput/cadence signals:

- `remote_batch_bytes_max=131072`, matching the new dispatch segment.
- During the stable part of the run, the active data relay's
  `max_remote_read_gap_ms` stayed around `190-203ms`, a major improvement over
  G6's multi-second read gaps.
- By the final relay close:
  `remote_to_global_rx_bytes=542597551`, `remote_reads=52306`, and
  `remote_batches=51231`.
- Main downlink admission moved almost all useful bytes:
  `send_slice_accepted=539681529`, `send_slice_errors=0`,
  `send_slice_zero=0`, and `send_slice_max_accepted=262144`.
- QUIC loss/congestion remained clean:
  `lost_bytes=0`, `congestion_events=0`, and
  `tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0)`.

Local egress service was active but still not the dominant data path:

- `tcp-local-egress-service windows=51404 cycles=4547 accepted_bytes=756644`
- `target_reached=3`
- `no_progress=4299`
- `hard_pause=36`
- `no_work=47066`

Tail risks that block final clean acceptance:

- A late TUN drop edge appeared:
  `tx_dropped_total=783`, `tx_dropped_delta=783`.
- TUN feedback did pause and resume:
  `tcp-tun-egress-feedback paused=true reason=drop_delta`,
  then `paused=false reason=pressure_low`.
- The data relay closed with terminal pending:
  `pending=2653878`,
  `terminal_pending_reap_bytes=2653878`,
  `close_pending_class=terminal_closed_no_send`,
  `close_egress_bytes=557386`.
- The relay close also reported a late read gap:
  `max_remote_read_gap_ms=3412`.
- The global RX queue reached the edge:
  `global_rx_queue_used_max=1019`,
  `global_rx_queue_capacity=1024`.

## Interpretation

The G6 diagnosis was directionally right: the local-only egress service was not
enough because useful payload admission still depended on the remote-payload
dispatch path. G7 shows that feeding each remote dispatch with one whole local
egress service window is enough to restore high data movement in this topology.

This is an important architecture result, not just a parameter tweak: it proves
the current branch can sustain `100+ Mbit/s` with ordered TUIC TCP when the
relay reader and main-loop admission cadence line up.

However, the run is not a final product acceptance because the tail still
overran local egress once:

- TUN drops were no longer zero.
- The global RX queue nearly filled.
- Close-tail reaped `2.65MB` of terminal pending data.

The next task should therefore not go back to VPS tuning, MTU/PLPMTUD, stale
pool, broad QUIC windows, or unordered reassembly. The next task is to keep the
new dispatch/egress cadence while making the tail clean.

## Next Acceptance Target

The next focused gate should be a repeat or small close-tail fix with the same
safe1200 reverse-first P1 shape.

Required result before calling Knife14 clean:

- receiver remains `>100 Mbit/s`;
- `tx_dropped_delta=0`;
- `terminal_pending_reap_bytes=0`;
- no `terminal_closed_no_send` close pending on the data relay;
- no global RX queue edge saturation;
- no QUIC loss/congestion/tx-blocking regression.

Only after that repeat passes should the project reopen broader `100+ Mbit/s`
stability work such as longer duration, P2/P4, or concurrency.
