# 2026-07-08 Knife14fy Ordered Read-Service Restore Results

## Scope

This run tested commit `653d62bf` after restoring TUIC TCP to the single ordered
`tokio::io::join(recv, send)` path and adding read-service/self-wake/startup
diagnostics.

The intended acceptance was a focused `.27 -> .33 -> .77` reverse-first P1
window with `safe1200`, TCP diagnostics enabled, and `MINI_VPN_TUIC_TCP_POOL=2`.
No VPS tuning, iperf3 changes, MTU/PLPMTUD work, stale-pool work, or broad QUIC
window changes were part of this stage.

## First-Attempt Artifacts

- Remote report:
  `/tmp/conn/mvpn_knife14fy_ordered_readsvc_p1_30_usclient_suite_20260708_115629.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14fy_ordered_readsvc_p1_30_usclient_suite_20260708_115629.tar.gz`
- Local report:
  `/tmp/mini_vpn/knife14fy_ordered_readsvc_p1_30/mvpn_knife14fy_ordered_readsvc_p1_30_usclient_suite_20260708_115629.md`
- Local bundle:
  `/tmp/mini_vpn/knife14fy_ordered_readsvc_p1_30/mvpn_knife14fy_ordered_readsvc_p1_30_usclient_suite_20260708_115629.tar.gz`
- Client log:
  `/tmp/conn/mvpn_accept_20260708_115629.log`

## Gates Before VPS Acceptance

Local gates passed on the Mac worktree:

- `cargo test --lib format_tuic_tcp_open_line_includes_target_pool_and_id`
- `cargo test --lib tuic`
- `cargo test --lib relay_read_service_diag_records_awaited_read_window_range`
- `cargo test --lib`
- `cargo build --release`
- `git diff --check`
- `rustfmt --edition 2024 --check src/tuic.rs src/client_tun.rs`

Remote focused gates passed on `.27` in a clean detached worktree at
`/home/ubuntu/mini_vpn_accept_knife14fy_20260708_035147`:

- `cargo test --lib tuic`
- `cargo test --lib relay_read_service_diag_records_awaited_read_window_range`
- `cargo test --lib`
- `cargo build --release`
- `git diff --check`

The `.27` default repo was intentionally not modified because it had unrelated
dirty worktree state. The acceptance worktree was created from fetched commit
`653d62bf`.

## Preflight Health

The exit VPS `.33` was active and still had the high-throughput socket-buffer
preflight values:

- `net.core.rmem_max=16777216`
- `net.core.wmem_max=16777216`
- `net.core.rmem_default=1048576`
- `net.core.wmem_default=1048576`

The target VPS `.77` iperf3 service was active.

The suite's direct baselines were healthy:

- `.27 -> .77` forward: `307 Mbit/s` sender, `277 Mbit/s` receiver.
- `.27 <- .77` reverse: `313 Mbit/s` sender, `281 Mbit/s` receiver.
- `.33 -> .77` forward: `323 Mbit/s` sender, `289 Mbit/s` receiver.
- `.33 <- .77` reverse: `322 Mbit/s` sender, `297 Mbit/s` receiver.

## First Acceptance Outcome

The acceptance did not reach the reverse-first iperf window. `client-tun`
failed during TUIC startup:

```text
tuic auth finish: sending stopped by peer: error 0
```

The suite's no-secret diagnostics after the failure showed:

- client and exit clocks matched;
- `sing-box` was active with `NRestarts=0`;
- UDP `:8443` was listening under `sing-box`;
- `sing-box -c /etc/sing-box/config.json check` exited `0`;
- no-secret UUID/password/SNI/ALPN match booleans were all `1`.

An explicit post-failure `.33` log tail did not show a TUIC inbound record in
the `11:56-11:59 CST` failure window. The latest TUIC records in the inspected
tail were from earlier successful tests around `11:13 CST`; later visible noise
was unrelated VLESS/REALITY invalid-handshake traffic.

## Follow-Up Startup Probes

After the first startup-auth failure, two bounded startup-only probes were run
on the same clean `.27` worktree and the same `653d62bf` binary:

- `MINI_VPN_TUIC_TCP_POOL=1`
  - log: `/tmp/conn/knife14fy_startup_pool1_20260708_053528.log`
  - outcome: startup succeeded, `✅ 已连接 TUIC 出口` and
    `🌊 UDP relay 数据面就绪` were present.
- `MINI_VPN_TUIC_TCP_POOL=2`
  - log: `/tmp/conn/knife14fy_startup_pool2_20260708_053626.log`
  - outcome: startup succeeded, the auxiliary TCP pool slot was established,
    and both QUIC stats streams were visible.

This demotes the first `tuic auth finish: sending stopped by peer: error 0`
result from a deterministic code failure to a transient startup/environment
failure unless it repeats in a future controlled run.

## Retry Acceptance Artifacts

- Remote report:
  `/tmp/conn/mvpn_knife14fy_ordered_readsvc_p1_30_retry1_usclient_suite_20260708_133658.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14fy_ordered_readsvc_p1_30_retry1_usclient_suite_20260708_133658.tar.gz`
- Local report:
  `/tmp/mini_vpn/knife14fy_ordered_readsvc_p1_30_retry1/mvpn_knife14fy_ordered_readsvc_p1_30_retry1_usclient_suite_20260708_133658.md`
- Local bundle:
  `/tmp/mini_vpn/knife14fy_ordered_readsvc_p1_30_retry1/mvpn_knife14fy_ordered_readsvc_p1_30_retry1_usclient_suite_20260708_133658.tar.gz`
- Client log:
  `/tmp/conn/mvpn_accept_20260708_133658.log`

## Retry Acceptance Outcome

The retry reached the reverse-first data plane and restored the current branch
from the Knife14fu no-data shape to a data-moving shape:

- reverse-first P1: `21.4 Mbit/s` sender, `20.0 Mbit/s` receiver.
- `throughput_shape=low_average`, `no_data=0`, `local_pressure=1`.
- `remote_to_global_rx_bytes=75502079`.
- `remote_reads=3319`.
- `remote_read_service_ticks=2763`.
- `remote_read_service_len_min=1200`, `remote_read_service_len_max=65536`.
- `tuic-open-tcp` showed `relay_mode=ordered_join` and
  `startup_auth_attempts=1` on the data stream.
- `tuic-tcp-stream-close` showed `self_wake_armed=9921` and
  `self_wake_fired=8857`.

Clean surfaces stayed clean:

- `pending_at_close=0`.
- `terminal_pending_reap=0`.
- `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=0`.
- QUIC loss, congestion, tx-blocking, and rx-blocking deltas were zero.

The limiting surfaces were local pressure and downlink backpressure:

- `downlink_backpressure pause_edges=3 resume_edges=2`.
- `send_queue_max=503160`.
- `may_recv_false=7655`.
- `budget_limited=7321`.
- `headroom_limited=7330`.
- `headroom_deferred_bytes=10188203`.
- `pressure_credit_blocked_bytes=960157`.
- final attribution:
  `final_headroom_limited+final_drain_credit+final_pressure_credit+final_hard_edge_guard`.

## Interpretation

The first run alone was not a throughput result and did not prove or disprove
the ordered read-service restoration. The startup-only probes and retry
acceptance completed that discriminator.

The ordered read-service restore succeeded at the stage goal: the current
branch is no longer in a no-data state. It now matches the expected next branch:
data moves in the tens of megabits range, while local pressure-credit and
downlink backpressure keep the reverse-first window below the `100+ Mbit/s`
target.

## Proposed Next Step

Wait for confirmation before code changes. The next code stage should target
local pressure-credit only:

1. Add focused tests for the pressure-credit/backpressure decision when the TUN
   send queue is high but kernel drops are zero and TCP send capacity remains
   available.
2. Preserve the existing hard safety invariants: bounded pending bytes, no
   terminal pending leak, no `send_slice_zero`/errors, and clean TUN drop
   accounting.
3. Adjust the local credit/headroom gate so short-lived TUN queue pressure does
   not suppress useful reverse downlink reads for multi-second windows when
   send capacity and drop signals are healthy.
4. Re-run local tests, then one focused reverse-first P1 acceptance. Success is
   data-moving with materially fewer `may_recv_false`,
   `headroom_deferred_bytes`, and `pressure_credit_blocked_bytes`, while keeping
   pending/terminal/TUN/QUIC surfaces clean.

Do not tune VPS settings, iperf3, MTU/PLPMTUD, stale pool, or broad QUIC windows
for the next stage unless new evidence contradicts the local pressure-credit
diagnosis.
