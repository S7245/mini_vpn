# Knife14as results - terminal pending is post-close, not the active-window root

Date: 2026-07-04

Code commit: `2205734`

Local bundle:
`/tmp/mini_vpn/mvpn_knife14as_terminal_pending_reversefirst2_usclient_suite_20260704_182136.tar.gz`

Extracted bundle:
`/tmp/mini_vpn/knife14as_terminal_pending_182136/`

Remote bundle:
`/tmp/conn/mvpn_knife14as_terminal_pending_reversefirst2_usclient_suite_20260704_182136.tar.gz`

Remote report:
`/tmp/conn/mvpn_knife14as_terminal_pending_reversefirst2_usclient_suite_20260704_182136.md`

Remote client log:
`/tmp/conn/mvpn_accept_20260704_182136.log`

## Verdict

Knife14as did not pass throughput acceptance, but it answered the lifecycle
question it was designed to answer.

The new terminal-pending accounting worked:

- close logs include the pre-abort local TCP socket snapshot;
- `terminal_pending_reap_bytes` is emitted when pending downlink is
  `Closed && !active && !can_send`;
- the low-RTT probe summary reports `terminal_pending_reap`.

The primary clean reverse-first window had low reverse throughput, but
`terminal_pending_reap=0` during the measurement. The terminal tail appeared
only after the local application closed the flow. That means the close tail is
an end-of-flow accounting outcome, not the direct active-window cause of the low
receiver throughput.

The active clean-window blocker is now local TUN egress loss plus downlink
backpressure:

- no clean-window QUIC loss/congestion;
- no `send_slice` zero/errors;
- no TUN flush syscall failures;
- no deferred immediate flush;
- no relay late-remote-after-local-finish;
- no global RX pressure;
- but `tun_tx_dropped_delta=703` and downlink pending reached the high watermark.

## Local Gates

Before the VPS run, local gates passed:

- `cargo test --lib terminal_pending_reap_bytes_classifies_closed_no_send_tail`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `cargo test --lib client_tun`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`
- `cargo test --lib`
- `cargo test`
- `cargo test --features harness --test concurrency_harness`
- `cargo clippy --all-targets --features harness -- -D warnings`

## VPS Setup

The `.27` suite rebuilt and tested commit `2205734` with a clean worktree.

Binary snapshot:

- binary: `/home/ubuntu/mini_vpn/target/release/mini_vpn`
- sha256:
  `a1c8ab82aab09f362507ec8219c9bae1bc3636bbe5a36301e2d7515d01badbd6`

Important run settings:

- `MINI_VPN_TUIC_CC=cubic`
- `RUN_REVERSE_FIRST_P1=1`
- `PARALLEL_SET=1`
- `MINI_VPN_TCP_DIAG=1`
- `MINI_VPN_TUN_MTU=1200`
- `MINI_VPN_TCP_RX_BUFFER_BYTES=1048576`
- `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`
- `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`
- `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`
- TUN queue length default: `500`

Direct baselines were healthy:

- `.27 -> .77`: sender `319 Mbit/s`, receiver `277 Mbit/s`
- `.77 -> .27 -R`: sender `308 Mbit/s`, receiver `277 Mbit/s`
- `.33 -> .77`: sender `309 Mbit/s`, receiver `286 Mbit/s`
- `.77 -> .33 -R`: sender `321 Mbit/s`, receiver `296 Mbit/s`

This rejects an unhealthy iperf3 target, unhealthy sing-box exit, or generally
slow exit-to-target path as the primary explanation.

## Primary Clean Reverse-First Signal

The reverse-first P1 window is the primary signal because it runs before the
standard forward pass can create inherited QUIC congestion.

Reverse-first P1:

- iperf: sender `22.600 Mbit/s`, receiver `22.200 Mbit/s`
- attribution: `local_tun_egress_drop+local_downlink_backpressure`
- downlink backpressure:
  `pause_edges=4`, `resume_edges=3`, `max_pending_bytes=564626`,
  `max_total_pending_bytes=564626`
- downlink flush:
  `attempts=17702`, `no_send_capacity=70`, `send_slice_calls=17632`,
  `accepted_bytes=78875952`, `zero=0`, `errors=0`,
  `budget_limited=585`, `max_accepted_bytes=65536`,
  `tun_flush_calls=16801`, `tun_flush_failures=0`,
  `tun_flush_deferred=0`, `remote_to_global_rx_bytes=78875952`
- terminal pending:
  `events=0`, `bytes=0`, `max_bytes=0`
- TUN drops:
  `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=703`
- QUIC:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_start_lost_bytes=0`, `max_start_congestion_events=0`,
  `min_start_cwnd=12000`, no blocked deltas
- relay:
  `global_rx_pressure_events=0`, `local_write_pressure_events=0`,
  `remote_after_local_finish_bytes=0`

Interpretation:

- terminal pending was absent during the clean measurement window;
- remote-to-local data reached mini_vpn (`remote_to_global_rx_bytes` advanced);
- smoltcp accepted payloads and TUN flush calls succeeded;
- packets were still dropped at local TUN egress;
- downlink high-water backpressure paused remote reads while local egress failed
  to sustain the incoming rate.

## Close Boundary Signal

Immediately after the primary window, the same reverse flow closed with an
explicit terminal pending tail:

```text
tcp-handle-close ... reason=uplink_channel_closed state=Relaying
pending=555611 pending_high=564626 remote_to_global_rx_bytes=83723363
send_slice_accepted=83167752 no_send_capacity=203
tun_flush_deferred=0 terminal_pending_reap_bytes=555611
tcp_state=Closed active=false can_send=false can_recv=false
```

That tail is no longer deliverable once the local socket is
`Closed && !can_send`. It is still important evidence, but its timing shows it
is the terminal consequence of the earlier active-window egress/backpressure
problem, not the direct cause of the low clean-window receiver rate.

## Secondary Windows

The standard and full runs are useful as corroborating evidence, but not as the
root signal because they inherited pressure from earlier forward passes.

Standard P1:

- forward: sender `34.100 Mbit/s`, receiver `27.700 Mbit/s`
- reverse: sender `20.300 Mbit/s`, receiver `19.400 Mbit/s`
- forward attribution:
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`
- later reverse attribution:
  `inherited_quic_congestion+local_tun_egress_drop+local_downlink_backpressure`

Full P1:

- forward: sender `4.160 Mbit/s`, receiver `1.360 Mbit/s`
- reverse: sender `25.100 Mbit/s`, receiver `24.400 Mbit/s`
- forward attribution:
  `quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop`
- later reverse attribution:
  `inherited_quic_congestion+local_tun_egress_drop+local_downlink_backpressure`

Terminal pending tails also appeared after these later windows, for example
`terminal_pending_reap_bytes=524294` and `terminal_pending_reap_bytes=535899`.
Those tails confirm the accounting path, but the inherited QUIC congestion means
they should not drive the next root-cause decision.

## Rejected Explanations

- Stale TUIC TCP pool slot: already closed by earlier accepted evidence, and no
  stale-slot timeout appeared in this run.
- iperf3 target or exit path health: rejected by healthy direct baselines.
- Clean-window QUIC congestion/loss: rejected by zero clean-window deltas.
- Forward-induced inherited congestion as the primary reverse root: rejected for
  the reverse-first window, which ran before the standard forward pass.
- TUN flush syscall failure: rejected by `tun_flush_failures=0`.
- `send_slice` failure or zero progress: rejected by `send_slice_zero=0` and
  `send_slice_errors=0`.
- Knife14aq-style deferred immediate flush: rejected by
  `tun_flush_deferred=0`.
- Terminal pending as the active-window cause: rejected by
  `terminal_pending_reap=0` during the clean reverse-first window.

## Architecture Implication

The core currently treats smoltcp send acceptance plus successful `flush_tx` as
the main proof that local downlink egress is keeping up. Knife14as shows that is
not enough on the Linux VPS path: the kernel TUN egress side can drop packets
after mini_vpn has accepted and flushed them, while mini_vpn simultaneously
enters downlink backpressure because the local TCP socket cannot accept remote
bytes fast enough.

The next change should not be another blind pacer, TUN queue length, TUIC pool,
iperf3, or sing-box tweak. It should re-evaluate the local egress/backpressure
architecture and add an explicit feedback path or bounded shaping rule that
connects actual TUN egress capacity to remote downlink reads.

## Proposed Next Stage

Because this was a failed acceptance run, do not start repair code
opportunistically. The next stage should be confirmed first.

Proposed Knife14at direction: local TUN egress feedback and drop-aware downlink
backpressure.

1. Write a short design tree comparing:
   - Linux-only TUN egress drop sampling as diagnostic feedback;
   - product-safe packet/egress pacing inside mini_vpn;
   - downlink backpressure changes that react to local egress pressure instead
     of only socket send capacity;
   - rejected queue-length-only tuning.
2. Add deterministic tests for the selected controller or accounting rule before
   changing runtime behavior.
3. Extend the probe report, if needed, to show TUN egress drop rate and
   downlink pause/resume timing in the same window.
4. Run one scoped reverse-first VPS acceptance only after local tests and a
   code-review pass.
