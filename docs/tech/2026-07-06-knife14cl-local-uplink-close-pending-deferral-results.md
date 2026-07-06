# Knife14cl Local Uplink Close Pending Deferral Results

Date: 2026-07-06

## Code

- Commit: `877c05a`
- Scope: local `uplink_channel_closed` now uses the same pending downlink
  close deferral as relay close. A new `tcp-deferred-close-pending` diagnostic
  marks the deferral instead of immediately rearming with useful pending bytes.

## Local Gates

- `cargo test pending_downlink_close_deferral --lib`
- `cargo test relay_closed_with_pending_downlink_defers_rearm --lib`
- `cargo test deferred_close_egress --lib`
- `cargo test reap --lib`
- `cargo test close --lib`
- `cargo test tun_rx_drain --lib`
- `cargo test pre_payload --lib`
- `cargo test pressure_credit --lib`
- `cargo test drop_credit --lib`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `cargo test --lib`
- `cargo test`
- `cargo build --release`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`
- `git diff --check`

Note: `cargo fmt --check` remains excluded as a stage gate because the
repository has pre-existing rustfmt drift across unrelated files.

## VPS Run

- Remote bundle:
  `/tmp/mini_vpn/knife14cl_local_close_pending_20260706_1852/mvpn_knife14cl_local_close_pending_usclient_suite_20260707_025232.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14cl_local_close_pending_20260706_1852_local/`

Baselines were healthy:

- `.27 -> .77`: `320/276 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `321/288 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `310/280 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `316/287 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 improved but failed acceptance:

- `iperf_sender_mbps=32.900`
- `iperf_receiver_mbps=31.900`
- `throughput_shape=low_average local_pressure=1`

Clean or improved signals:

- Throughput left the previous 10-20 Mbit/s band.
- `pending_at_close=0`
- `egress_at_close=0`
- `terminal_pending_reap=0`
- `terminal_late_remote_payload=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- QUIC loss/congestion/blocking deltas stayed zero.
- Current `.33` logs showed normal TUIC inbound entries for `.27`; no current
  `fail auth` evidence. REALITY invalid-connection lines were unrelated public
  scan noise on another inbound.

Knife14cl-specific success signal:

- The previous close-tail hidden rearm changed into explicit deferral:
  `tcp-deferred-close-pending handle=SocketHandle(1)
  direction=local_to_remote reason=uplink_channel_closed pending=528364`.

Remaining failure signals:

- Pending still did not drain during the post-iperf close tail:
  `pending_total=528364 pending_high=528364 dirty_handles=1`.
- TUN drops returned:
  parser `tun_tx_dropped_delta=4604`; runtime feedback
  `drop_events=2 drop_delta_total=4334 max_delta=2714`.
- Feedback paused and did not resume:
  `tun_egress_feedback pause_edges=2 resume_edges=0`.
- Drop and pressure credit were installed but not paid:
  `drop_credit_debt_bytes=167956 drop_credit_debt_paid_bytes=0`;
  `pressure_credit_debt_bytes=28652 pressure_credit_debt_paid_bytes=0`;
  `pressure_credit_blocked_bytes=24576`.
- Local egress stayed at the credit edge:
  `send_queue_max=892928`, `may_recv_false=8333`,
  `headroom_limited=8444`, `headroom_deferred_bytes=2178611350`.
- ACK drain stayed active and mostly reached `would_block`:
  `tun_rx_drain attempts=349 packets=11679 budget_exhausted=23
  would_block=326`.

## Interpretation

Knife14cl fixed a real lifecycle bug. The local `uplink_channel_closed` path no
longer rearms while send-capable pending downlink exists, and the parser no
longer reports `pending_at_close` or `egress_at_close` as hidden loss points.

The result also exposed the next bottleneck. Once local egress reaches
`send_queue=892928` and TUN drop feedback installs debt, pending stays dirty
but no debt is paid and no resume occurs. The remaining branch is not close
accounting, stale pool, sing-box, QUIC loss, or ACK-drain reachability; it is
the local pressure recovery algorithm after TUN drop/credit debt when pending
bytes are still useful and the socket remains send-capable.

## Next

Knife14cm should repair the credit-debt recovery path algorithmically rather
than widening thresholds. The next test should prove that observed local egress
drain can retire drop/pressure debt and release bounded flush credit, so a
dirty send-capable pending backlog can make forward progress instead of
remaining paused with `drop_credit_debt_paid_bytes=0`.
