# Knife14ce Drop-Aware Egress Credit Results

Date: 2026-07-06 / VPS local time 2026-07-07 00:50 CST

## Code Under Test

- Branch: `codex/knife14d-downlink-reap-open`
- Base local HEAD before the stage: `1d52e34`
  (`test(knife14cd): record default pool2 VPS acceptance`)
- Client VPS worktree: `/home/ubuntu/mini_vpn`
- The VPS run used a synchronized working tree containing the Knife14ce code
  and script changes, not a committed SHA on the VPS.
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Remote bundle:
  `/tmp/mini_vpn/knife14ce_drop_credit_20260707_0049/mvpn_knife14ce_drop_credit_usclient_suite_20260707_005007.tar.gz`
- Local extracted copy:
  `/tmp/mini_vpn/knife14ce_drop_credit_20260707_0049_local/`

## Local Gates Before VPS

- `cargo test drop_credit --lib`
- `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test tcp_downlink_flush_aggregate_formats_progress_signal --lib`
- `cargo test tun_egress_feedback_diag_formats_transition_counters --lib`
- `cargo test downlink_egress_clock --lib`
- `cargo test --lib` (`306` passed)
- `cargo test` (`306` passed)
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## Run Shape

- Suite tag: `knife14ce_drop_credit`
- Probe: clean reverse-first P1 only
- MTU: `1200`
- Parallel: `1`
- Duration: `30s`
- TCP diag: enabled
- Server evidence: enabled
- Direct target reverse baseline: required
- Exit-to-target baseline: required
- No manual `MINI_VPN_TUIC_TCP_POOL` override was supplied. Startup confirmed
  the default pool=2 behavior with data/control split across connections.

## Preflight

- `.27` had a VPS-local `.env`; the suite had to source it explicitly.
- `.33` sing-box was active and NTP synchronized.
- `.77` iperf3 was active before the run; later target SSH evidence collection
  timed out after the probe, but direct preflight iperf baselines had already
  passed.
- Direct `.27 -> .77` baseline was healthy:
  - forward receiver: `277 Mbit/s`
  - reverse receiver: `274 Mbit/s`
- Exit `.33 -> .77` baseline was healthy:
  - forward receiver: `256 Mbit/s`
  - reverse receiver: `286 Mbit/s`
- Current-window `.33` sing-box evidence showed TUIC inbound/direct outbound
  entries for `43.130.32.77:5201` at `2026-07-07 00:51:02 +0800`.
- No current-window TUIC `fail auth` appeared. The unrelated errors in the tail
  were VLESS REALITY invalid connections from other source IPs.

## Result

Knife14ce failed VPS acceptance.

- Reverse-first P1 throughput:
  - sender: `23.6 Mbit/s`
  - receiver: `21.5 Mbit/s`
  - `throughput_shape=shape=low_average local_pressure=1 no_data=0 stable_high=0`
- Pool isolation stayed healthy:
  - `tcp_pool: opens=2 conns=0,1 reconnects=0 reasons=none`
- Clean transport surfaces stayed clean:
  - QUIC loss/congestion/blocking deltas: `0`
  - `global_rx_pressure events=0`
  - `send_slice_zero=0`
  - `send_slice_errors=0`
  - `tun_flush_failures=0`
- Close/reap did not hide pending loss:
  - `pending_at_close=0`
  - `egress_at_close=0`
  - `terminal_pending_reap=0`

## Negative Signals

- TUN egress drops remained:
  - probe summary `tun_tx_dropped_delta=2813`
  - final lifecycle `drop_delta_total=5405`
- Feedback pause did not recover in the final lifecycle:
  - probe summary `pause_edges=2 resume_edges=0`
  - final lifecycle `pause_edges=11 resume_edges=0`
- The new drop-credit flush counters stayed zero:
  - `drop_credit_debt_bytes=0`
  - `drop_credit_debt_paid_bytes=0`
  - `drop_credit_blocked_bytes=0`
- The feedback line did install global debt:
  - `drop_credit_generation=1 drop_credit_debt_bytes=196608`
  - then later `drop_credit_generation=11 drop_credit_debt_bytes=196608`
- However the first positive TUN drop feedback appeared near the close tail,
  after the hot downlink burst had already filled local egress pressure:
  - `tcp_reverse_window ... send_queue=851955`
  - `tcp-tun-egress ... tx_dropped_delta=933`
- The final aggregate still showed pressure at the credit edge:
  - `send_queue_max=892928`
  - `headroom_limited=1146`
  - `drain_credit_granted_bytes=74876313`
  - `drain_credit_planned_bytes=4182322`
  - `hard_edge_guard_limited=1146`
- TUIC data delivery was not a no-data failure, but it was bursty:
  - data stream reached about `81.6 MB`
  - repeated `tuic-tcp-stream-pending` and `tuic-tcp-stream-read-gap` lines
    appeared before and around the first drop.

## Interpretation

The drop-debt model is correct as a local invariant, but the signal arrived too
late for this VPS failure. The debt was installed after the harmful burst had
already filled the local tx queue and after useful downlink flushing had mostly
stopped. That is why `tcp-tun-egress-feedback` showed a nonzero
`drop_credit_debt_bytes`, while `tcp-downlink-flush` and final lifecycle
drop-credit counters stayed zero.

This run should not reopen stale pool, iperf3, sing-box auth/time/config, QUIC
loss/congestion, terminal pending, or close/reap branches. The active branch is
still local TCP downlink egress credit/backpressure behavior, but the next
control point must move earlier than sysfs `tx_dropped` sampling.

## Next Bias

- Keep default TUIC TCP pool=2.
- Keep the drop-debt invariant, but do not rely on post-drop debt as the first
  control signal.
- Add proactive credit debt on local egress pressure edges, before TUN drops are
  observed.
- Fix or prove the feedback pause/resume path so a post-drop pause cannot remain
  stuck when raw local pressure has drained.
- Require the next VPS run to show either:
  - stable high throughput with materially lower TUN drops; or
  - clear counters proving proactive credit debt engaged and why it still was
    insufficient.

## Progress

- Overall Knife14 estimate after this run: `83%`.
- Reason: high throughput was already proven by Knife14cd, but Knife14ce did
  not stabilize it. The new evidence narrowed the gap to earlier local
  egress-pressure credit gating and feedback unwind rather than close/reap loss.
