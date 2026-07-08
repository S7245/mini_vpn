# 2026-07-08 Knife14fz Soft Pressure-Credit Results

## Goal

Validate the narrow soft-target-edge pressure-credit change from the sing-box
comparison stage:

- keep the current TUIC ordered reverse-first path;
- treat the derived egress target edge as a soft warning;
- avoid installing pressure debt or hard flush clamps before the stronger
  credit/pause/drop/flush-failure evidence appears;
- do not change VPS config, iperf3, MTU/PLPMTUD, stale pool behavior, or broad
  QUIC windows.

## Code Under Test

- Branch: `codex/knife14d-downlink-reap-open`
- Commit: `52f2bae0` (`fix(knife14fz): soften target-edge pressure credit`)
- Remote worktree:
  `/home/ubuntu/mini_vpn_accept_knife14fz_20260708_52f2bae`

Implementation summary:

- `reaches_downlink_credit_debt_pressure_threshold(...)` now waits for
  `tx_queue_credit_spend_threshold(cfg)` instead of the lower target edge.
- projected pressure debt also waits for the credit spend threshold.
- clean no-debt flush credit may spend up to the credit spend threshold;
  active debt still uses the lower target edge.

## TDD And Gates

Local gates passed:

- `cargo test --lib downlink_pressure_credit_debt`
- `cargo test --lib pressure`
- `cargo test --lib downlink_flush`
- `cargo test --lib credit`
- `cargo test --lib` (`396 passed`)
- `rustfmt --edition 2024 --check src/client_tun.rs`
- `git diff --check`
- `cargo build --release`

Remote `.27` focused gates passed in the clean worktree:

- `cargo test --lib pressure` (`36 passed`)
- `cargo test --lib` (`396 passed`)
- `cargo build --release`
- `git diff --check`
- `rustfmt --edition 2024 --check src/client_tun.rs`

VPS preflight before acceptance:

- `.33` `sing-box`: `active`
- `.33` socket buffers:
  - `net.core.rmem_max=16777216`
  - `net.core.wmem_max=16777216`
  - `net.core.rmem_default=1048576`
  - `net.core.wmem_default=1048576`
- `.77` `iperf3`: `active`

## VPS Acceptance

Command shape on `.27`:

- sourced `/home/ubuntu/mini_vpn/.env` from the clean worktree without copying
  or printing secrets;
- ran `scripts/knife14b-usclient-tunnel-suite.sh`;
- `SUITE_TAG=knife14fz_soft_pressure_p1_30`;
- `RUN_REVERSE_FIRST_P1=1`;
- `STOP_AFTER_REVERSE_FIRST_P1=1`;
- `MTU=1200`;
- `DURATION=30`;
- `IPERF_TIMEOUT_SECS=120`;
- `MINI_VPN_TUIC_TCP_POOL=2`;
- `MINI_VPN_TCP_DIAG=1`.

Artifacts:

- Remote bundle:
  `/tmp/conn/mvpn_knife14fz_soft_pressure_p1_30_usclient_suite_20260708_141720.tar.gz`
- Local bundle:
  `/tmp/mini_vpn/knife14fz_soft_pressure_p1_30/mvpn_knife14fz_soft_pressure_p1_30_usclient_suite_20260708_141720.tar.gz`
- Report:
  `/tmp/mini_vpn/knife14fz_soft_pressure_p1_30/mvpn_knife14fz_soft_pressure_p1_30_usclient_suite_20260708_141720.md`
- Client log:
  `/tmp/mini_vpn/knife14fz_soft_pressure_p1_30/mvpn_accept_knife14fz_soft_pressure_p1_30_20260708_141720.log`

Direct baseline in the suite:

- forward receiver: `278 Mbit/s`
- reverse receiver: `278 Mbit/s`

mini_vpn reverse-first P1:

- sender: `88.0 MBytes`, `24.6 Mbit/s`, `Retr=22`
- receiver: `81.9 MBytes`, `22.9 Mbit/s`
- shape: low average with burst/idle intervals, not no-data

Per-second shape:

- early data movement: `30.4`, `29.4`, `23.1`, `21.0 Mbit/s`
- long idle window: seconds `5-11` mostly `0`
- bursts: `182`, `66.1`, `81.8`, `90.1`, `110 Mbit/s`
- average remains in the old `20 Mbit/s` band.

## Attribution Signals

The change successfully moved the current branch away from the Knife14fu
no-data shape:

- `remote_to_global_rx_bytes=85881616`
- `send_slice_accepted=85881616`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `may_recv_false=0`
- `pending_at_close=0`
- `terminal_pending_reap=0`
- TUN rx/tx drop deltas `0`
- QUIC loss/congestion/blocking deltas `0`

The remaining low-throughput attribution is still local, but it is now more
specific than "target-edge debt":

- suite attribution: `local_pressure_credit+local_downlink_backpressure`
- `downlink_backpressure pause_edges=1 resume_edges=1`
- `max_pending_bytes=348144`
- `max_tx_queue_bytes=534144`
- `max_total_pressure_bytes=882288`
- `headroom_deferred_bytes=363494`
- `pressure_credit_debt_bytes=122727`
- `pressure_credit_blocked_bytes=161309`
- `drain_credit_granted_bytes=82670684`
- `drain_credit_used_bytes=156114`
- `tun_flush_deferred=4`
- `send_queue_max=534144`
- `global_rx_pressure_events=0`
- `local_write_pressure_events=0`

TUIC stream/read-service evidence:

- data stream `remote_reads=10425`
- data stream `remote_read_service_ticks=10032`
- data stream `remote_batch_limited=14`
- data stream `read_credit_pause_updates=1`
- data stream `read_credit_limit_bytes_min=65536`
- `tuic_stream_pending events=14`
- all pending events were `connection_stream_frames_pending`
- `data_pending_gap_max_ms=3006`
- `data_read_gap_max_ms=3522`
- `self_wake_armed=10142`
- `self_wake_fired=6957`
- QUIC connection had stream frames and no transport blocking during those
  gaps.

## Conclusion

The soft target-edge change is a useful first repair: the current branch is
again data-moving under clean reverse-first P1, and the no-data `0.046 Mbit/s`
Knife14fu failure is no longer the active shape.

It is not enough to reach the target. The branch remains near `22.9 Mbit/s`
receiver, far below the `100+ Mbit/s` target and the mature sing-box client's
`173/173 Mbit/s` window.

The next root is no longer a single premature target-edge debt trigger. The
evidence now points to a combined local pressure/read-service cadence problem:
brief downlink backpressure/pause appears, then the TUIC stream repeatedly sits
with `connection_stream_frames_pending` for about `3s` before another burst
arrives. That burst/idle cadence is what keeps the average in the old band.

## Proposed Next Code Slice

Do not proceed by tuning VPS, iperf3, MTU/PLPMTUD, stale pools, broad QUIC
windows, or more target-edge constants.

The next code stage should be a focused TDD slice around egress-progress
feedback into TUIC stream read service:

1. Add a deterministic test where local egress clears pressure after a brief
   pause and proves the relay reader is re-woken immediately, not only by a
   coarse pending/self-wake cadence.
2. Add a test where `connection_stream_frames_pending` plus available local
   send capacity forces a bounded immediate read-service tick.
3. Keep hard safety: no reads past true pause/drop/flush-failure debt, bounded
   pending, no close-drain regression.
4. Implement a small "egress progress feedback" path from successful local
   flush/drain-credit payment into the remote read-service wake path.
5. Rerun the same focused safe1200 reverse-first P1 acceptance only after the
   tests prove the feedback path.

Acceptance for the next slice should require:

- reverse-first P1 materially above the `20-25 Mbit/s` band;
- fewer multi-second `connection_stream_frames_pending` gaps;
- no increase in TUN drops, `send_slice_zero`, `send_slice_errors`,
  `pending_at_close`, or QUIC loss/blocking;
- ideally reduced `read_credit_pause_updates`,
  `pressure_credit_blocked_bytes`, and `headroom_deferred_bytes`.

