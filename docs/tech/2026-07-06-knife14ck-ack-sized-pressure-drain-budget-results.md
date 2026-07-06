# Knife14ck ACK-Sized Pressure Drain Budget Results

Date: 2026-07-06

## Code

- Commit: `d69ee27`
- Scope: adaptive pressure TUN RX drain now sizes its default packet budget
  against small ACK/window packets instead of MTU-sized data packets, with a
  hard cap of `256` packets/pass. Explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET`
  remains an override.

## Local Gates

- `cargo test tun_rx_drain --lib`
- `cargo test tun_rx_pressure --lib`
- `cargo test pre_payload --lib`
- `cargo test maintenance --lib`
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
  `/tmp/mini_vpn/knife14ck_ack_sized_drain_20260707_0240/mvpn_knife14ck_ack_sized_drain_usclient_suite_20260707_024026.tar.gz`
- Local extracted bundle:
  `/tmp/mini_vpn/knife14ck_ack_sized_drain_20260707_0240_local/`
- Startup confirmed the intended default:
  `pressure-adaptive=256 packets near tx_queue_credit_high`.

Baselines were healthy:

- `.27 -> .77`: `346/285 Mbit/s` forward sender/receiver.
- `.27 <- .77`: `325/294 Mbit/s` reverse sender/receiver.
- `.33 -> .77`: `323/294 Mbit/s` forward sender/receiver.
- `.33 <- .77`: `311/282 Mbit/s` reverse sender/receiver.

Tunnel reverse-first P1 failed:

- `iperf_sender_mbps=16.500`
- `iperf_receiver_mbps=15.700`
- `throughput_shape=low_average local_pressure=1`

Clean or narrowed signals:

- QUIC loss/congestion/blocking deltas stayed zero.
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- `terminal_pending_reap=0`
- `terminal_late_remote_payload=0`
- No current `.33` `fail auth` evidence was found in the post-run auth check.

Knife14ck-specific success signals:

- Parser summary: `tun_rx_drain attempts=764 packets=2483 tcp=2483
  budget_exhausted=1 would_block=763 errors=0`.
- Raw source counters showed all new paths were reachable:
  `pre_payload_attempts=369 remote_payload_attempts=381
  maintenance_attempts=14 other_attempts=0`.
- Runtime TUN drop feedback improved from Knife14cj's thousands-level drops to
  `drop_events=1 drop_delta_total=273 max_delta=273`.

Remaining failure signals:

- Parser TUN drop delta still reported `tun_tx_dropped_delta=813`; runtime
  feedback saw `273`, so local TUN egress loss is reduced but not gone.
- `pending_at_close`: `events=1 bytes=524906`, all
  `send_capable_events=1`.
- `egress_at_close`: `events=1 bytes=892928`, all
  `send_capable_events=1`, with `drain_candidate_events=1`.
- Final close line:
  `close_pending_class=active_send_capable close_pending_bytes=524906
  close_egress_class=active_send_capable close_egress_bytes=892928
  close_egress_drain_candidate=true tcp_state=CloseWait can_send=true
  may_send=true may_recv=false`.
- `downlink_flush`: `send_queue_max=892928`, `may_recv_false=1509`,
  `hard_edge_guard_limited=983`, `tun_flush_deferred=170`,
  `pending_high=524906`.
- Pressure credit installed only near the close tail:
  `tcp-egress-credit-debt reason=pressure_edge installed_bytes=25194
  max_pending=524906 max_tx_queue=892928`.
- Data stream delivery was real but bursty:
  `data_rx_bytes_max=61927496`, `data_read_gap_max_ms=6092`,
  `data_pending_gap_max_ms=6092`.

## Interpretation

Knife14ck fixed the specific budget bug from Knife14cj. The drain now reaches
`would_block` almost every time it runs, so the ready ACK/window backlog is no
longer being left unread simply because the packet budget is too small.

That did not lift throughput out of the 10-20 Mbit/s band. The remaining
evidence points away from further ACK-drain budget work and toward local
egress/close-drain lifecycle: useful bytes are still queued while the socket is
`CloseWait`, active, send-capable, and explicitly marked as an egress drain
candidate. The low throughput is therefore not hidden terminal reap, QUIC
loss, sing-box auth, or missing ACK-drain reachability.

## Next

Knife14cl should stop increasing ACK drain budget and inspect the dirty-relay /
egress flush cadence plus close deferral path. The next patch should prove that
active send-capable TUN egress backlog keeps receiving flush/drain opportunities
until it falls below a safe threshold or the socket is no longer useful,
instead of disappearing into close-tail accounting.
