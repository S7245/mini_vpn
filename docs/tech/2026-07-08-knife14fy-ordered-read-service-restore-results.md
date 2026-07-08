# 2026-07-08 Knife14fy Ordered Read-Service Restore Results

## Scope

This run tested commit `653d62bf` after restoring TUIC TCP to the single ordered
`tokio::io::join(recv, send)` path and adding read-service/self-wake/startup
diagnostics.

The intended acceptance was a focused `.27 -> .33 -> .77` reverse-first P1
window with `safe1200`, TCP diagnostics enabled, and `MINI_VPN_TUIC_TCP_POOL=2`.
No VPS tuning, iperf3 changes, MTU/PLPMTUD work, stale-pool work, or broad QUIC
window changes were part of this stage.

## Artifacts

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

## Acceptance Outcome

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

## Interpretation

This run is not a throughput result and does not prove or disprove the ordered
read-service restoration. It only proves that this acceptance attempt was
blocked before the data plane could be exercised.

The current branch did not materially change the primary 1-RTT authenticate
path compared with `f8765c1`; the code change there is diagnostic accounting for
auxiliary TCP pool startup attempts. Because the failure happened while
finishing TUIC Authenticate and produced no contemporaneous sing-box TUIC
inbound log, the next discriminator must isolate startup/auth repeatability
before changing read cadence, egress credit, or throughput controls.

## Proposed Next Step

Wait for confirmation before further execution or code changes. The next safe
diagnostic should be startup-only:

1. Run a short `.27` startup probe on the same `653d62bf` clean worktree with
   `MINI_VPN_TUIC_TCP_POOL=1`, no iperf window, and a bounded timeout.
2. If pool `1` starts, repeat startup-only with `MINI_VPN_TUIC_TCP_POOL=2` to
   separate primary authenticate from auxiliary-slot behavior.
3. If startup succeeds, rerun the same focused reverse-first P1 acceptance once.
4. If startup still fails, run the same startup-only probe on a clean
   `f8765c1` worktree before touching code. Only if `f8765c1` starts and
   `653d62bf` does not should the next code change instrument or adjust the
   startup authenticate/open-uni path.

Do not tune VPS settings, iperf3, MTU/PLPMTUD, stale pool, or broad QUIC windows
for this failure unless new evidence contradicts the startup-auth diagnosis.
