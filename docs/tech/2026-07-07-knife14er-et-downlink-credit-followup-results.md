# Knife14er-es-et downlink credit follow-up results

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Goal

Continue the Knife14 local downlink credit controller work after Knife14eq, with
one-hour stop discipline: avoid more stale pool, iperf3, sing-box, egress pacer,
or QUIC MTU loops unless new evidence overturns them.

Acceptance target remained clean reverse-first P1 at `100+ Mbit/s` receiver with
zero terminal pending reap, zero pending/egress at close, no TUN drops, and no
QUIC loss/blocking.

## Local code changes tested

1. Knife14er half-closed gap hint:
   `should_poll_relay_ack_drain_hint` now keeps ACK/window maintenance active
   after local writer finish while the remote read side is still alive.

2. Knife14es floor-aware pressure credit:
   predictive relay read credit just below the flush edge no longer collapses
   to tiny residual headroom like `24B`; it keeps at least the ACK/window floor.

3. Knife14et progress-aware staging:
   with headroom debt but observed egress progress, controller staging remains
   at high-water scale; only consecutive no-progress egress collapses staging
   to the smaller pressure limit. Controller also refuses to publish sub-MTU
   staging residual credit.

Focused TDD and full local gates passed for these changes:

- `cargo test --lib --quiet`
- `cargo test --features harness --quiet`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `cargo build --release --quiet`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Remote `.27` focused gate also passed:

- `cargo test --lib downlink_credit_controller --quiet`
- `cargo test --lib relay_read_credit --quiet`
- `cargo test --lib relay_ack_drain_hint --quiet`
- `cargo build --release --quiet`

## VPS runs

### Knife14er half-closed gap hint

Bundle:
`/tmp/mini_vpn/knife14er_half_closed_gap_hint_p1_30/mvpn_knife14er_half_closed_gap_hint_p1_30_usclient_suite_20260707_182258.tar.gz`

Outcome: failed acceptance at `22.8/21.7 Mbit/s`.

Useful signal:

- No-data shape was cleared: `no_data=0`.
- Half-closed hint was active:
  `relay_gap_hint_attempts=48`, `relay_gap_hint_followup_attempts=144`.
- No late terminal leakage:
  `relay_late_remote post_finish_bytes=0`, `terminal_pending_reap=0`,
  `pending_at_close=0`, `egress_at_close=0`.
- TUN and QUIC remained clean:
  `tun_tx_dropped_delta=0`, QUIC loss/congestion/blocking `0`.

Failure shape shifted back to local pressure:

- `downlink_backpressure pause_edges=2 resume_edges=1`
- `send_queue_max=557386`
- `may_recv_false=14729`
- `headroom_deferred_bytes=23669193`
- `read_credit_limit_bytes_min=24`

Interpretation: allowing half-closed gap hints fixed the no-data branch but
left a tiny residual-headroom read-credit loop.

### Knife14es floor-aware credit

Bundle:
`/tmp/mini_vpn/knife14es_floor_aware_credit_p1_30/mvpn_knife14es_floor_aware_credit_p1_30_usclient_suite_20260707_182841.tar.gz`

Outcome: failed acceptance at `23.5/21.7 Mbit/s`.

Useful signal:

- Tiny `24B` credit was eliminated during the active window:
  `read_credit_limit_bytes_min` stayed around `1200` before later tail samples,
  then post-close tail reached `123`.
- Pending stayed lower than prior local-pressure runs:
  `pending_total_max=113756`.
- TUN and QUIC stayed clean:
  `tun_tx_dropped_delta=0`, QUIC loss/congestion/blocking `0`.
- Close/reap stayed clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`.

Remaining pressure symptoms:

- `may_recv_false=14883`
- `headroom_deferred_bytes=37959890`
- `send_queue_max=553336`
- Attribution remained `local_pressure_credit+local_downlink_backpressure`.

Interpretation: sub-MTU predictive credit was not the whole bottleneck. The
controller still oscillated between small receive windows and delayed flush
progress.

### Knife14et progress-aware staging

First attempt failed before tunnel P1 because the command enabled
`EXIT_TO_TARGET_IPERF_CHECK=1` without explicit `EXIT_SSH_HOST` /
`EXIT_SSH_KEY`. That was an invocation error, not a tunnel result.

Valid bundle:
`/tmp/mini_vpn/knife14et_progress_staging_p1_30b/mvpn_knife14et_progress_staging_p1_30b_usclient_suite_20260707_183653.tar.gz`

Outcome: failed acceptance at `0.769/0.042 Mbit/s`.

Baselines were healthy:

- Client `.27 -> .77`: reverse receiver `281 Mbit/s`
- Exit `.33 -> .77`: reverse receiver `281 Mbit/s`

Useful clean signal:

- Local pressure disappeared:
  `downlink_backpressure pause_edges=0 resume_edges=0 max_pressure_bytes=0`
- Flush/backlog was clean:
  `pending_total_max=0`, `headroom_deferred_bytes=0`,
  `pressure_credit_blocked_bytes=0`
- Read credit never collapsed:
  `read_credit_limit_bytes_min=65536`
- TUN and QUIC stayed clean:
  `tun_tx_dropped_delta=0`, QUIC loss/congestion/blocking `0`
- Close/reap stayed clean:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`

New/returned failure shape:

- `throughput_shape: shape=no_data local_pressure=0 no_data=1`
- Data stream only received `210912B`
- `tuic_tcp_stream data_read_gap_max_ms=20500`
- `tuic_stream_pending data_pending_gap_max_ms=20383`
- `relay_late_remote post_finish_bytes=53888`
- Attribution:
  `late_remote_after_local_finish+tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap+target_sender_stalled+tuic_stream_starved`

Interpretation: this is not evidence for another headroom/read-credit threshold
tweak. The pressure loop was effectively removed, but reverse throughput got
worse because the target sender/TUIC stream was starved before local pressure
existed.

## Cause classification

Code issue:

- The last progress-aware staging change is likely not acceptable as-is. It
  removed local pressure symptoms, but pushed the run back into the no-data /
  stream starvation branch. Keep it uncommitted until a better architecture
  explains the interaction.

Framework/architecture issue:

- The current controller is trying to solve two different loops in one local
  pressure policy: smoltcp/TUN egress backlog and remote reverse TCP
  ACK/window cadence. The Knife14et result shows these can be decoupled:
  egress can be clean while the remote sender is still starved.

Environment issue:

- Not the main root for the valid run. Direct and exit-to-target baselines were
  healthy, services were active, safe1200 was active, and QUIC loss/blocking
  remained zero.

Test/invocation issue:

- The first Knife14et suite attempt was invalid due to missing exit SSH
  settings while exit-to-target preflight was required.

## Decision

Pause here instead of continuing to tweak thresholds. The next stage should
not be another local pressure constant change. It should either:

1. add deterministic diagnostics/tests around reverse TCP ACK propagation and
   local FIN ordering after `CloseWait`, or
2. split the controller into two explicit loops:
   a local egress pressure loop and an ACK/window cadence loop, with separate
   observability for upstream ACK bytes, TUN RX drain source, and TUIC stream
   read wakeups.

The project is still roughly at the high 90% implementation state for the
current branch mechanics, but acceptance is blocked until the ACK/window
cadence branch is explained.
