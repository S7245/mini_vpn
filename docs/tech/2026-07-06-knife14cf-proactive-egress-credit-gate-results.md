# Knife14cf Proactive Egress Credit Gate Results

Date: 2026-07-06 / VPS local time 2026-07-07 01:12 CST

## Code Under Test

- Branch: `codex/knife14d-downlink-reap-open`
- Base local HEAD before the stage: `12e2785`
  (`test(knife14ce): record drop-aware egress credit evidence`)
- Client VPS worktree: `/home/ubuntu/mini_vpn`
- The VPS run used a synchronized working tree containing the Knife14cf code
  and script changes, not a committed SHA on the VPS.
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Remote bundle:
  `/tmp/mini_vpn/knife14cf_pressure_credit_20260707_0112/mvpn_knife14cf_pressure_credit_usclient_suite_20260707_011213.tar.gz`
- Local extracted copy:
  `/tmp/mini_vpn/knife14cf_pressure_credit_20260707_0112_local/`

## Local Gates Before VPS

- `cargo test pressure_credit --lib`
- `cargo test credit_debt --lib`
- `cargo test tun_egress_feedback --lib`
- `cargo test drop_credit --lib`
- `cargo test downlink_egress_clock --lib`
- `cargo test tcp_downlink_flush_aggregate_formats_progress_signal --lib`
- `cargo test tcp_downlink_diag_tracks_headroom_limited_flushes --lib`
- `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib` (`310` passed)
- `cargo test` (`310` passed)
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

## Run Shape

- Suite tag: `knife14cf_pressure_credit`
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

- `.27` had a VPS-local `.env`; the suite sourced it explicitly.
- `.33` sing-box was active and NTP synchronized.
- `.77` iperf3 was active and NTP synchronized.
- Direct `.27 -> .77` baseline was healthy:
  - forward receiver: `288 Mbit/s`
  - reverse receiver: `281 Mbit/s`
- Exit `.33 -> .77` baseline was healthy:
  - forward receiver: `281 Mbit/s`
  - reverse receiver: `296 Mbit/s`
- Current-window `.33` sing-box evidence showed TUIC inbound/direct outbound
  entries for `43.130.32.77:5201` at `2026-07-07 01:13:09 +0800`.
- No current-window TUIC `fail auth` appeared. The only nearby unrelated error
  was a VLESS REALITY invalid connection from another source IP.

## Result

Knife14cf failed VPS acceptance.

- Reverse-first P1 throughput:
  - sender: `20.5 Mbit/s`
  - receiver: `19.5 Mbit/s`
  - `throughput_shape=shape=low_average local_pressure=1 no_data=0 stable_high=0`
- Pool isolation stayed healthy:
  - `tcp_pool: opens=2 conns=0,1 reconnects=0 reasons=none`
- Clean transport surfaces stayed clean:
  - QUIC loss/congestion/blocking deltas: `0`
  - `global_rx_pressure events=0`
  - `send_slice_zero=0`
  - `send_slice_errors=0`
  - `tun_flush_failures=0`

## Positive Signals

- Proactive pressure debt engaged before drops:
  - `tcp-egress-credit-debt reason=pressure_edge`
  - pressure generations advanced repeatedly up to the hot close tail
  - probe flush summary reported
    `pressure_credit_debt_bytes=195593`,
    `pressure_credit_debt_paid_bytes=3342336`, and
    `pressure_credit_blocked_bytes=3735552`
- TUN drops improved compared with Knife14ce:
  - Knife14ce probe `tun_tx_dropped_delta=2813`
  - Knife14cf probe `tun_tx_dropped_delta=539`
  - Knife14cf final `drop_delta_total=1081`
- Feedback pause recovered at least once during the clean probe:
  - probe `pause_edges=1 resume_edges=1`
  - final lifecycle `pause_edges=3 resume_edges=2`

## Negative Signals

- Throughput stayed in the same low class:
  - many iperf intervals were `0.00 bits/sec`
  - receiver average remained `19.5 Mbit/s`
- Local tx-queue pressure still reached the hard credit edge:
  - `send_queue_max=892928`
  - `headroom_limited=5539` in the probe summary
  - `hard_edge_guard_limited=952` in the probe summary
- Whole-suite close still had active send-capable backlog:
  - `final_pending_at_close bytes=574203 send_capable_bytes=574203`
  - `final_egress_at_close bytes=892928 drain_candidate_bytes=892928`
  - `final_terminal_pending_reap bytes=0`
- The data TUIC stream was live but bursty:
  - `data_rx_bytes_max=60017102` in the probe summary
  - final stream close showed `rx_bytes=77188392`
  - final data stream gaps reached `max_read_gap_ms=4325`
- The final close line stayed active and send-capable rather than terminal:
  - `tcp_state=CloseWait active=true can_send=true can_recv=false`
  - `close_pending_class=active_send_capable`
  - `close_egress_class=active_send_capable`

## Interpretation

The Knife14cf algorithm did what it was designed to do: it moved credit denial
earlier than sysfs TUN drop feedback, made pressure credit visible, reduced the
drop count, and proved the feedback gate can resume from raw low pressure.

That was not sufficient to restore throughput. The remaining failure is not
stale pool, iperf3, sing-box auth/time/config, QUIC loss/congestion, TUN flush
syscall failure, `send_slice` failure, or hidden terminal pending reap. The
pressure debt reduced one damage signal but left the main burst/stall cadence
intact: the local smoltcp send queue repeatedly reached the credit edge, useful
reverse throughput came in bursts, and the TUIC data stream still saw multi-
second read gaps.

The next patch should stop adding more static debt/threshold tuning. The most
likely remaining coupling is that local egress pressure pauses the receive path
too coarsely, which can feed back into TUIC stream read cadence and the remote
TCP sender. The next design stage should decouple remote read progress from
local TUN egress bursts with bounded per-flow accounting, or explicitly prove
that this architecture is not the blocker.

## Next Bias

- Keep default TUIC TCP pool=2.
- Keep pressure/drop credit observability, but do not treat more credit debt as
  the final fix.
- Add an architecture-level test/patch for bounded receive-path decoupling:
  the relay should be able to keep the TUIC stream/window making progress while
  local TUN egress is paused, without creating unbounded app-owned pending.
- Preserve close/reap accounting: active send-capable close backlog must remain
  visible until it disappears in a stable high-throughput run.

## Progress

- Overall Knife14 estimate after this run: `85%`.
- Reason: Knife14cf closed the "post-drop feedback is stuck/too late" subbranch
  and improved TUN-drop/feedback evidence, but did not move throughput out of
  the `10-20 Mbit/s` class or eliminate active send-capable close backlog.
