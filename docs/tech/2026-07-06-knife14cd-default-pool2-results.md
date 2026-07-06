# Knife14cd Default Pool=2 Results

Date: 2026-07-06 / VPS local time 2026-07-07 00:28 CST

## Code Under Test

- Commit: `f219044` (`fix(knife14cd): isolate TUIC TCP streams by default`)
- Includes prior semantic commit:
  `3135d68` (`fix(knife14cd): flush relay writer payload batches`)
- Branch: `codex/knife14d-downlink-reap-open`
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Bundle:
  `/tmp/mini_vpn/knife14cd_default_pool2_20260707_0028/mvpn_knife14cd_default_pool2_usclient_suite_20260707_002800.tar.gz`
- Local extracted copy:
  `/tmp/mini_vpn/knife14cd_default_pool2_20260707_0028_local/extract/`

## Local Gates Before VPS

- `cargo test tcp_pool --lib`
- `cargo test relay_writer --lib`
- `cargo test relay_remote_read_progresses_while_local_write_is_pending --lib`
- `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib` (`304` passed)
- `cargo test`
- `git diff --check`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`

## Run Shape

- Suite tag: `knife14cd_default_pool2`
- Probe: clean reverse-first P1 only
- MTU: `1200`
- Parallel: `1`
- Duration: `30s`
- TCP diag: enabled
- Server evidence: enabled
- Direct target reverse baseline: required
- Exit-to-target baseline: required
- No manual `MINI_VPN_TUIC_TCP_POOL` override was supplied to the suite. The
  suite default and product default both selected pool=2.

## Preflight

- `.27` fast-forwarded cleanly to `f219044`.
- Startup confirmed `TUIC TCP connection pool=2`.
- Direct `.27 -> .77` baseline was healthy:
  - forward receiver: `264 Mbit/s`
  - reverse receiver: `281 Mbit/s`
- Exit `.33 -> .77` baseline was healthy:
  - forward receiver: `284 Mbit/s`
  - reverse receiver: `286 Mbit/s`
- `.33` sing-box and `.77` iperf3 services were active, and NTP was
  synchronized on both hosts.
- Current-window `.33` evidence showed TUIC inbound/direct outbound entries for
  `43.130.32.77:5201`; no current-window TUIC `fail auth` was found.

## Positive Signals

- Reverse-first P1 reached high throughput:
  - sender: `180 Mbit/s`
  - receiver: `179 Mbit/s`
  - `throughput_shape=shape=stable_high`
- The pool isolation target was met:
  - `tcp_pool: opens=2 conns=0,1 reconnects=0 reasons=none`
  - control stream opened on `conn=0`
  - data stream opened on `conn=1`
  - data stream `data_first_rx_max_ms=3`
  - data stream `data_rx_bytes_max=674859752`
- The no-data branch is closed for this default run:
  - `no_data=0`
  - no slow data first byte
  - no tiny data-stream byte count
- QUIC path remained clean in the low-RTT summary:
  - `max_lost_bytes_delta=0`
  - `max_congestion_events_delta=0`
  - `max_tx_blocked_data_delta=0`
  - `max_rx_blocked_data_delta=0`
- Data integrity/lifecycle guardrails stayed explicit:
  - `send_slice_zero=0`
  - `send_slice_errors=0`
  - `tun_flush_failures=0`
  - `terminal_pending_reap=0`

## Remaining Negative Signals

This is not a full Knife14 acceptance. The run restored throughput but exposed
the local egress/drop limiter more strongly:

- TUN egress drops:
  - `tun_tx_dropped_delta=6754`
  - `runtime_tun_egress: drop_events=8 drop_delta_total=6754 max_delta=1723`
  - `final_tun_egress_feedback: pause_edges=8 resume_edges=8`
- Downlink pressure still oscillated:
  - `downlink_backpressure: pause_edges=317 resume_edges=317`
  - `max_pending_bytes=588452`
  - `max_tx_queue_bytes=892928`
  - `max_pressure_bytes=892928`
- The final close tail still had send-capable backlog:
  - `final_pending_at_close: events=1 bytes=7680`
  - `final_egress_at_close: events=1 bytes=892928`
  - `drain_candidate_events=1`
- Credit/guard counters show the remaining algorithmic limiter:
  - `headroom_limited=70114`
  - `headroom_deferred_bytes=13789155739`
  - `drain_credit_granted_bytes=422705483`
  - `drain_credit_planned_bytes=240771954`
  - `drain_credit_used_bytes=240771954`
  - `hard_edge_guard_limited=70114`
  - `hard_edge_guard_deferred_bytes=1683517811`
  - `tun_flush_deferred=2537`
- Final attribution:
  `final_pending_at_close+final_pending_send_capable+final_egress_at_close+final_egress_drain_candidate+final_runtime_tun_egress_drop+final_tun_egress_feedback_drop+final_headroom_limited+final_drain_credit+final_hard_edge_guard`

## Interpretation

Knife14cd succeeds at the scoped pool objective: product/suite default pool=2
prevents the pool=1 same-connection no-data failure and returns reverse-first
P1 to a high-throughput range.

It does not finish Knife14. The active remaining branch is mini_vpn local
downlink egress/drop control under high throughput. The final close/reap
signals are no longer hidden terminal pending loss; they are send-capable
backlog and TUN egress pressure at the credit hard edge.

## Next Bias

- Keep pool=2 default; do not increase pool size from this evidence.
- Do not reopen sing-box auth/time/config, iperf3, stale pool, QUIC
  loss/congestion, or script-noise branches.
- The next behavior patch should be algorithmic and drop-aware:
  - treat TUN drop feedback as credit debt or a temporary credit freeze;
  - stop granting/spending egress drain credit as if all local drain were clean
    immediately after drops;
  - keep pending bounded and visible; and
  - require final TUN drops and final egress-at-close to clean up before
    calling Knife14 stable.

## Progress

- Overall Knife14 estimate after this run: `88%`.
- Reason: the throughput/no-data/first-byte branch is now closed under default
  config, but final TUN drops and egress-at-close backlog remain blocking.
