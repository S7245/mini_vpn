# Knife14bo Terminal Pending Close-Drain Results

Date: 2026-07-05

## Code Under Test

- Commit: `615cf47` (`fix(knife14bo): gate terminal late downlink payload`)
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Remote bundle:
  `/tmp/mini_vpn/knife14bo_terminal_late_20260705/mvpn_knife14bo_terminal_late_615cf47_usclient_suite_20260705_184932.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14bo_terminal_late_20260705_184932/`

## Local Gates

- `cargo test --lib remote_payload_ -- --nocapture`
- `cargo test --lib tcp_downlink_diag_tracks_pending_acceptance_and_flush_failures -- --nocapture`
- `cargo test --lib tcp_downlink_flush_aggregate_formats_progress_signal -- --nocapture`
- `scripts/knife14b-lowrtt-probe.sh --self-test`
- sandbox-external `cargo test --lib`: `285 passed`
- `git diff --check`

`cargo fmt --check` is still not a valid narrow-stage gate on this branch
because historical formatting differences span unrelated files.

## VPS Run Settings

- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `SERVER_EVIDENCE_CHECK=1`
- `MINI_VPN_TUIC_CC=cubic`
- `PARALLEL_SET=1`
- `MINI_VPN_TCP_DIAG=1`
- `MINI_VPN_TUN_MTU=1200`
- `MINI_VPN_TCP_RX_BUFFER_BYTES=1048576`
- `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`
- `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`
- `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`

Preflight:

- `.27`, `.33`, and `.77` NTP synchronized.
- `.33` sing-box active.
- `.77` iperf3 active.
- Direct `.27 -> .77`: `313/274 Mbit/s`.
- Direct `.27 <- .77`: `311/279 Mbit/s`.

## Tunnel Result

- Reverse-first P1 over mini_vpn: `25.3/24.0 Mbit/s`.
- This is still below Knife14 acceptance and has not escaped the low-throughput
  class.

## Key Signals

Local mini_vpn:

- `downlink_backpressure`: `pause_edges=16`, `resume_edges=15`;
  `max_pending_bytes=0`, `max_tx_queue_bytes=587504`.
- `downlink_flush`: `accepted_bytes=90234895`, `pending_total_max=0`,
  `pending_high=65536`, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`, `tun_flush_deferred=15`.
- `terminal_pending_reap`: `0`.
- `terminal_late_remote_payload`: `0`.
- `pending_at_close`: `0`.
- `tun_tx_dropped_delta`: `0`.
- `tun_egress_feedback`: `pause_edges=0`, `drop_events=0`.
- QUIC loss/congestion: `0/0`.
- Data stream timing: first RX `3ms`, data read gap max `4035ms`,
  data pending gap max `4019ms`, data RX max `90282126B`.

Server-side/manual follow-up:

- The suite did not set `EXIT_SSH_HOST` or `TARGET_SSH_HOST`, so its
  server-evidence artifact skipped `.33` and `.77` collection.
- Manual `.33` window check for `2026-07-05 18:51 +0800` showed TUIC inbound
  from `.27` and direct outbound opens to `.77:5201`; no `fail auth` was found.
- Manual `.77` iperf3 journal for the same window showed the target reverse
  sender itself at `90.6 MBytes / 25.3 Mbit/s` with bursty zero-throughput
  seconds. This matches the tunnel sender result.

## Classification

Knife14bo fixed the hidden-loss observability gap it targeted:

- terminal no-send late payload is now observable;
- this run had no terminal-late payload;
- this run had no terminal pending reap;
- this run had no pending-at-close.

Therefore the current `24 Mbit/s` reverse-first failure is not a hidden
terminal close/reap loss point.

The active evidence now points to a pre-close receive-window / ACK / tx-queue
pressure branch:

- mini_vpn accepted and flushed about `90MB`, matching the target sender volume;
- the target sender itself was slow and bursty;
- local app-owned pending stayed zero, while smoltcp tx queue repeatedly crossed
  the backpressure high watermark;
- TUN qdisc drops and QUIC congestion stayed zero.

## Next Plan Proposal

Do not patch another behavior branch until this plan is confirmed.

1. Fix the suite evidence parameters so `SERVER_EVIDENCE_CHECK=1` actually
   collects `.33` sing-box and `.77` iperf3 journal data by default when the
   known SSH hosts/keys are available.
2. Start Knife14bp as an evidence-first branch for tx-queue-only backpressure
   and receive-window behavior:
   - distinguish smoltcp tx-queue pressure from app-owned pending;
   - record whether `global_rx` pauses caused by tx queue alone correlate with
     target sender stalls;
   - keep terminal pending/late payload as regression gates.
3. Only after that evidence, consider a bounded behavior change to tx-queue-only
   backpressure. Do not return to stale pool, sing-box auth, iperf3 blame, or
   egress pacer tuning unless the new evidence changes the branch.
