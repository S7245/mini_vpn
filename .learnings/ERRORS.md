# Errors

## 2026-07-06 - Knife14cq remote sync must target repository subdirectories

- Stage: Knife14cq `.27` sync before VPS acceptance.
- Symptom: an initial `rsync` sent `src/client_tun.rs` and docs to
  `/home/ubuntu/mini_vpn/` instead of `src/` and `docs/tech/`.
- Resolution: the misplaced files created by this sync were removed, then
  `src/client_tun.rs` and docs were copied to their exact target directories.
- Correct behavior: when syncing scoped source to `.27`, use explicit remote
  paths such as `/home/ubuntu/mini_vpn/src/client_tun.rs` and
  `/home/ubuntu/mini_vpn/docs/tech/`, or use a tested `--relative` pattern.
  Verify the remote source hash before building.

## 2026-07-06 - Knife14cq clippy caught a nested if in the opt-in gate

- Stage: Knife14cq local gates.
- Symptom: `cargo clippy --all-targets --features harness -- -D warnings`
  failed with `clippy::collapsible-if` in the recent-active timer deadline
  gate.
- Resolution: collapse the condition to `has_downlink_work && let
  Some(duration) = ...`, then rerun focused tests, clippy, full tests, release
  build, harness, and `git diff --check`.
- Correct behavior: keep clippy in the Knife14 gate set after code changes that
  touch runtime control flow; small style failures are cheap to fix before VPS.

## 2026-07-06 - Knife14cp VPS regressed after recent-active timer ACK drain engaged

- Stage: Knife14cp recent-active timer ACK drain VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_retry_20260706_035508/mvpn_knife14cp_recent_active_timer_retry_usclient_suite_20260707_035508.tar.gz`
- Symptom: reverse-first P1 reached only `19.2/18.0 Mbit/s`, worse than
  Knife14co's `25.5/24.3 Mbit/s`, while still `low_average`.
- Important discriminator: the new code path definitely engaged:
  `timer_active_flow_attempts=1321`, `tun_rx_drain attempts=10633`,
  `would_block=10542`, and `errors=0`.
- Clean surfaces: `local_pressure=0`, `downlink_backpressure pause_edges=0`,
  `tun_tx_dropped_delta=0`, runtime `drop_delta_total=0`,
  `pending_at_close=0`, `egress_at_close=0`, `terminal_pending_reap=0`,
  `terminal_late_remote_payload=0`, no QUIC loss/congestion/blocking deltas,
  no send-slice or TUN flush errors, and no current `.33` TUIC `fail auth`.
- Rejected next moves: do not keep extending recent-active timer drain windows,
  increasing below-pressure ACK budgets, or adding more timer ACK-drain
  variants from this evidence.
- Correct behavior: remove or disable this path as a default, keep the evidence
  as an A/B rejection, and continue with the no-pressure burst/idle branch at
  the TUIC stream read/wake or remote-to-local scheduling boundary.

## 2026-07-06 - Knife14cp suite setup needs explicit exit SSH when server evidence is off

- Stage: first Knife14cp VPS suite attempt.
- Failed bundle:
  `/tmp/mini_vpn/knife14cp_recent_active_timer_20260706_035349/mvpn_knife14cp_recent_active_timer_usclient_suite_20260707_035349.tar.gz`
- Symptom: the suite failed before P1 because `SERVER_EVIDENCE_CHECK=0` skipped
  default SSH host setup, but `EXIT_TO_TARGET_IPERF_CHECK=1` still required
  exit-side SSH variables.
- Correct behavior: when disabling server evidence to avoid `.77:22` tail
  pollution, explicitly set `EXIT_SSH_HOST=ubuntu@43.153.32.33` and
  `EXIT_SSH_KEY=/home/ubuntu/.ssh/vpn` if exit-side baseline checks remain on.

## 2026-07-06 - Knife14co VPS still failed after active-flow drain removed local pressure

- Stage: Knife14co active-flow ACK drain VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14co_active_flow_ack_20260706_034114/mvpn_knife14co_active_flow_ack_usclient_suite_20260707_034114.tar.gz`
- Symptom: reverse-first P1 improved to `25.5/24.3 Mbit/s` but remained
  `low_average`.
- Important discriminator: the failure was no longer local pressure during the
  probe window. The parser reported `local_pressure=0`,
  `downlink_backpressure pause_edges=0`, `tun_tx_dropped_delta=0`,
  runtime `drop_delta_total=0`, and `send_queue_max=427496`.
- Active-flow ACK drain was engaged and bounded:
  `tun_rx_drain attempts=7506 packets=24055 tcp=24055 budget_exhausted=15
  would_block=7491 errors=0`.
- Clean surfaces: direct `.27/.33 <-> .77` baselines were healthy, current
  `.33` logs had no `fail auth`, QUIC loss/congestion/blocking deltas were
  zero, pending/close/reap accounting stayed clean, and there were no
  send-slice or TUN flush errors.
- Remaining root direction: do not keep adding pressure/drop debt or ACK budget
  size. The next repair should test a recent-active timer drain below pressure
  so ACK/window updates generated after a burst can be drained even when no new
  remote payload arrives to trigger event-driven drain.

## 2026-07-06 - Knife14co server evidence target SSH polluted post-probe client log tail

- Stage: Knife14co VPS acceptance evidence collection.
- Bundle:
  `/tmp/mini_vpn/knife14co_active_flow_ack_20260706_034114/mvpn_knife14co_active_flow_ack_usclient_suite_20260707_034114.tar.gz`
- Symptom: after the P1 attribution summary, the client log showed a new
  `tuic-open-tcp target=43.130.32.77:22` flow and later pressure/drop lines.
- Root cause: the suite collects target `.77` SSH evidence while the
  `43.130.32.77/32` route is still installed through `tun0`, so the evidence
  SSH session itself enters mini_vpn and perturbs the client log tail.
- Correct behavior: for Knife14 attribution, trust the probe-window summary and
  treat target-evidence tail pressure as test noise unless the same signals
  appear inside the P1 window. Future suite improvements should remove the
  target route before target SSH evidence or skip target SSH evidence for
  reverse-first stop runs.

## 2026-07-06 - cargo fmt check is not a safe Knife14 gate in the current worktree

- Stage: Knife14co local gates.
- Symptom: an extra `cargo fmt --check` failed with broad formatting diffs in
  many pre-existing files, far beyond the Knife14co patch.
- Correct behavior: do not run `cargo fmt` during Knife14 unless a dedicated
  formatting task is requested. Keep using `git diff --check`, focused tests,
  full tests, release build, harness, and clippy as the stage gates.

## 2026-07-06 - Knife14cl VPS failed after local-close deferral exposed unpaid drop debt

- Stage: Knife14cl local uplink close pending deferral VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cl_local_close_pending_20260706_1852/mvpn_knife14cl_local_close_pending_usclient_suite_20260707_025232.tar.gz`
- Symptom: reverse-first P1 improved to `32.9/31.9 Mbit/s` but remained
  `low_average local_pressure=1`.
- Important discriminator: the code change worked. The data flow produced
  `tcp-deferred-close-pending ... direction=local_to_remote
  reason=uplink_channel_closed pending=528364`, while parser close accounting
  reported `pending_at_close=0` and `egress_at_close=0`.
- Remaining local loss/backlog: TUN egress drops returned
  (`tun_tx_dropped_delta=4604`, runtime `drop_delta_total=4334`), pending stayed
  dirty at `528364`, and feedback paused without resume.
- Debt dead-end: final downlink diagnostics had
  `drop_credit_debt_bytes=167956 drop_credit_debt_paid_bytes=0` and
  `pressure_credit_debt_bytes=28652 pressure_credit_debt_paid_bytes=0`, while
  `send_queue_max=892928`, `may_recv_false=8333`, and
  `headroom_limited=8444`.
- Clean surfaces: no current `.33` `fail auth`, no QUIC loss/congestion/blocking
  deltas, no send-slice zero/errors, no TUN flush failure, no terminal pending
  reap, and no terminal late remote payload.
- Rejected next moves: do not continue close-accounting edits, ACK drain budget
  increases, stale pool work, sing-box auth/time/config work, iperf3 tuning,
  receive-window expansion, or static threshold widening from this evidence.
- Correct behavior: repair pressure debt recovery so observed local egress
  drain can retire drop/pressure debt and release bounded flush credit for
  dirty send-capable pending bytes.

## 2026-07-06 - Knife14ck VPS failed after ACK-sized drain reached would_block

- Stage: Knife14ck ACK-sized pressure drain budget VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14ck_ack_sized_drain_20260707_0240/mvpn_knife14ck_ack_sized_drain_usclient_suite_20260707_024026.tar.gz`
- Symptom: reverse-first P1 reached only `16.5/15.7 Mbit/s`, with
  `throughput_shape=low_average local_pressure=1`, despite healthy direct
  `.27 -> .77`, `.27 <- .77`, `.33 -> .77`, and `.33 <- .77` baselines.
- Important discriminator: ACK-sized pressure drain was no longer starved by
  budget. The final summary showed `tun_rx_drain attempts=764`,
  `budget_exhausted=1`, and `would_block=763`.
- Remaining local loss/backlog: runtime TUN egress feedback saw
  `drop_events=1 drop_delta_total=273`, and final close still had active
  send-capable backlog with `pending=524906`, `send_queue=892928`,
  `close_egress_drain_candidate=true`, `tcp_state=CloseWait`,
  `can_send=true`, and `may_send=true`.
- Clean surfaces: no QUIC loss/congestion/blocking deltas, no send-slice
  zero/errors, no TUN flush failure, no terminal pending reap, no terminal late
  remote payload, and no current `.33` `fail auth` evidence.
- Rejected next moves: do not keep raising the TUN RX drain budget or tuning
  static ACK-drain values. Do not redirect this result to iperf3, sing-box
  config/time/auth, stale pool slots, QUIC congestion, or receive-window
  expansion.
- Correct behavior: inspect and repair dirty-relay egress retention and
  close-drain lifecycle so active send-capable queued bytes keep receiving
  bounded flush opportunities before close/reap.

## 2026-07-06 - Knife14ci VPS failed because ACK drain runs after the burst

- Stage: Knife14ci adaptive TUN RX ACK drain VPS acceptance.
- Failed/diagnostic bundles:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_20260707_0208/mvpn_knife14ci_adaptive_ack_drain_usclient_suite_20260707_020847.tar.gz`,
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_retry_20260707_0212/mvpn_knife14ci_adaptive_ack_drain_retry_usclient_suite_20260707_021233.tar.gz`
- Symptom: the retry P1 stayed `low_average` at `18.8/17.9 Mbit/s` despite
  healthy direct `.27 -> .77` and `.33 -> .77` baselines.
- Important discriminator: `tcp-tun-rx-drain attempts=5 packets=105 tcp=105
  budget_exhausted=5`, so ACK/window packets were ready and the new code path
  engaged. The failure is not "no ACK drain"; it is "ACK drain happens too late
  and only on remote-payload events."
- Local loss/backlog: `tun_tx_dropped_delta=82`, `send_queue_max=892928`,
  `hard_edge_guard_limited=51`, and close-tail active send-capable backlog
  (`pending=525514`, `close_egress_bytes=892928`).
- Clean surfaces: no QUIC loss/congestion/blocking deltas, no send-slice
  zero/errors, no TUN flush failure, no terminal pending reap, no terminal late
  remote payload.
- Correct behavior: add pressure-gated pre-payload and maintenance TUN RX drain
  before more remote bytes are accepted/flushed. Keep it bounded and off below
  the existing egress credit edge.

## 2026-07-06 - Knife14ci startup auth failure was transient, not config mismatch

- Stage: first Knife14ci VPS suite attempt.
- Failed bundle:
  `/tmp/mini_vpn/knife14ci_adaptive_ack_drain_20260707_0208/mvpn_knife14ci_adaptive_ack_drain_usclient_suite_20260707_020847.tar.gz`
- Symptom: client-tun exited during startup with
  `tuic auth finish: sending stopped by peer: error 0`.
- Checks: `.33` sing-box was active with `NRestarts=0`, config check passed,
  `.27` and `.33` time skew was `0s`, NTP was synchronized, and no-secret
  comparison showed UUID/password/ALPN/SNI all matched exactly.
- Resolution: a 20s client-tun smoke immediately after the failure connected
  successfully, and the retry suite reached P1. Treat this as a transient
  TUIC/QUIC entry failure unless it repeats three times in a row.
- Correct behavior: when this appears, run no-secret config/time checks and a
  short startup smoke before restarting sing-box or changing mini_vpn code.

## 2026-07-06 - .27 non-login shell lacks cargo and rg

- Stage: Knife14ci `.27` verification.
- Symptom: `ssh ... 'cargo test ...'` failed with `cargo: command not found`;
  `rg` was also unavailable on `.27`.
- Correct behavior: use `source ~/.cargo/env && cd /home/ubuntu/mini_vpn` for
  remote cargo commands, and use `grep` on `.27` unless `rg` is installed.

## 2026-07-06 - Knife14ch VPS failed before pressure debt could prove throughput

- Stage: Knife14ch adaptive pressure credit debt VPS acceptance.
- Failed bundles:
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_20260707_0142/mvpn_knife14ch_adaptive_pressure_usclient_suite_20260707_014241.tar.gz`,
  `/tmp/mini_vpn/knife14ch_adaptive_pressure_repeat_20260707_0149/mvpn_knife14ch_adaptive_pressure_repeat_usclient_suite_20260707_014902.tar.gz`
- Symptom A: the first run was `no_data` (`0.245/0.020 Mbit/s`) with
  `pressure_credit_debt_bytes=0`, no local pressure, no TUN drops, and no
  current TUIC `fail auth`. It did not exercise the code path being tested.
- Symptom B: the repeat, with `.33 -> .77` baseline forced on, returned to
  `low_average` (`16.0/15.5 Mbit/s`) while direct baselines stayed healthy:
  `.27 -> .77` `282/298 Mbit/s`, `.33 -> .77` `266/297 Mbit/s`.
- Important discriminator: the repeat had clean QUIC loss/congestion/blocking
  deltas, no TUN drops, no global RX queue pressure, and no send-slice errors,
  but still reached `send_queue_max=892928`, `hard_edge_guard_limited=47`, and
  `terminal_late_remote_payload_bytes=1834980`.
- Root cause direction: adaptive pressure debt was not rejected directly; it
  was not installed. The remaining local branch is ACK/window/TUN-RX drain
  cadence under sustained remote payload, plus the close lifecycle consequence
  when the socket reaches terminal no-send while the relay still has tail data.
- Rejected next moves: do not attribute this run to sing-box auth/time/config,
  iperf3, stale pool, QUIC congestion/loss, bounded receive-window growth, or
  another static pressure-debt variant.
- Correct behavior: add a focused, pressure-triggered TUN RX drain path that
  activates only near the egress credit edge and keeps explicit
  `MINI_VPN_TUN_RX_DRAIN_BUDGET` as an override/test knob.

## 2026-07-06 - Knife14cg VPS failed because receive decoupling inflated local backlog

- Stage: Knife14cg bounded global RX receive-window VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cg_global_rx_receive_20260707_0126/mvpn_knife14cg_global_rx_receive_usclient_suite_20260707_012632.tar.gz`
- Symptom: reverse-first P1 reached only `20.0/18.7 Mbit/s` with
  `throughput_shape=low_average`, despite healthy direct `.27 -> .77` and
  `.33 -> .77` baselines.
- Important discriminator: `global_rx_receive` paused at the intended receive
  bound (`receive_high=2097152`, `max_pending_bytes=2123091`), proving the A/B
  path was active. The result was larger app-owned pending, not higher
  throughput.
- Local loss/backlog: final lifecycle showed active send-capable pending
  (`pending=2123091`) and close egress backlog (`close_egress_bytes=892928`),
  with final TUN egress drops totaling `4051`.
- Rejected next moves: do not keep raising receive windows, split receive
  thresholds, pool size, iperf3, sing-box auth/time/config, QUIC congestion, or
  stale pool logic from this evidence.
- Correct behavior: gate bounded global receive decoupling behind
  `MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1` and keep the product default on
  the safe receive gate. The next behavior patch must target local egress
  drain/cadence and prove that queued bytes are consumed rather than buffered
  into larger pending.

## 2026-07-06 - Knife14cf VPS failed after proactive pressure credit reduced drops

- Stage: Knife14cf proactive egress credit gate VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14cf_pressure_credit_20260707_0112/mvpn_knife14cf_pressure_credit_usclient_suite_20260707_011213.tar.gz`
- Symptom: reverse-first P1 reached only `20.5/19.5 Mbit/s`, with many
  zero-throughput iperf intervals, even though direct `.27 -> .77` and
  `.33 -> .77` baselines were healthy.
- Important discriminator: proactive pressure debt engaged and reduced probe
  TUN drops (`tun_tx_dropped_delta=539`, down from Knife14ce's `2813`), and the
  feedback gate recovered during the probe (`pause_edges=1 resume_edges=1`).
- Remaining root for this stage: local burst/stall cadence still reached the
  hard smoltcp tx-queue edge (`send_queue_max=892928`) and closed with active
  send-capable backlog (`pending=574203`, `close_egress_bytes=892928`).
- Rejected next moves: do not keep adding static pressure/drop debt, stale pool
  changes, iperf3 tuning, sing-box auth/time/config work, QUIC congestion work,
  TUN queue length tuning, or close/reap hiding.
- Correct behavior: re-evaluate the receive-path architecture. The next patch
  should test bounded decoupling between TUIC stream reads/global receive
  progress and local TUN egress pressure, while keeping per-flow pending and
  close accounting bounded and observable.

## 2026-07-06 - Knife14cf rsync must preserve repository paths and SSH key

- Stage: Knife14cf local-to-`.27` sync.
- Symptom A: a first `rsync` attempt without `-i ~/.ssh/vpn` failed with SSH
  `Permission denied`.
- Correct behavior A: use `rsync -e 'ssh -i ~/.ssh/vpn ...'` for `.27`, `.33`,
  and `.77` syncs from the Mac mini.
- Symptom B: a second sync without `-R` copied selected files into
  `/home/ubuntu/mini_vpn/` root instead of their repository subdirectories.
- Correct behavior B: for focused file syncs, use `rsync -avR` from the repo
  root or explicit destination directories. Remove any accidental root-level
  copies before running VPS tests so the remote worktree stays understandable.

## 2026-07-06 - Knife14ce VPS failed because drop-debt feedback arrived after the hot burst

- Stage: Knife14ce drop-aware egress credit VPS acceptance.
- Failed bundle:
  `/tmp/mini_vpn/knife14ce_drop_credit_20260707_0049/mvpn_knife14ce_drop_credit_usclient_suite_20260707_005007.tar.gz`
- Symptom: reverse-first P1 reached only `23.6/21.5 Mbit/s` with
  `tun_tx_dropped_delta=2813` in the probe and final `drop_delta_total=5405`.
- Important discriminator: `tcp-tun-egress-feedback` installed
  `drop_credit_debt_bytes=196608`, but the hot `tcp-downlink-flush` and final
  lifecycle summaries kept `drop_credit_debt_bytes=0`,
  `drop_credit_debt_paid_bytes=0`, and `drop_credit_blocked_bytes=0`.
- Root cause for this stage: sysfs TUN drop feedback was sampled too late to
  control the burst that had already filled local egress pressure; it became
  close-tail evidence rather than hot-path control.
- Rejected next moves: do not treat this as stale pool, iperf3, sing-box auth,
  time skew, server config, QUIC loss/congestion, terminal pending, or
  close/reap loss.
- Correct behavior: keep the debt invariant, but add a proactive local
  egress-pressure credit gate before another VPS run. Also verify feedback can
  resume from raw low pressure instead of remaining stuck after drops.

## 2026-07-06 - Knife14ce remote commands need login shell and explicit env source

- Stage: Knife14ce `.27` sync and VPS acceptance setup.
- Symptom A: a direct non-login SSH command on `.27` failed with
  `cargo: command not found`.
- Correct behavior A: use `bash -lc 'cd /home/ubuntu/mini_vpn && cargo ...'`
  for remote Rust commands so the VPS toolchain environment is loaded.
- Symptom B: the first suite attempt stopped before the business test because
  required TUIC environment variables were not present in the shell even though
  `.env` existed on `.27`.
- Correct behavior B: in the same true TTY command that runs the suite, source
  the VPS-local `.env` with `set -a; . ./.env; set +a` before invoking the
  script. Do not print or store any secret values.

## 2026-07-06 - Knife14bw VPS acceptance failed with tx-queue pressure oscillation

- Stage: Knife14bw reverse starvation diagnostics acceptance for commit
  `4a12b18`.
- Failed bundle:
  `/tmp/mini_vpn/knife14bw_starvation_diag_20260706/mvpn_knife14bw_starvation_diag_usclient_suite_20260706_212704.tar.gz`
- Symptom: reverse-first P1 reached only `19.8/18.9 Mbit/s`, with burst/idle
  intervals rather than stable high throughput.
- Important discriminator: data stream delivery was not tiny
  (`tuic_tcp_stream data_rx_bytes_max=72662685`), and live
  `tcp_reverse_window` samples showed `send_capacity=1048576`, `pending=0`,
  `active=true`, `can_send=true`, and mostly `may_recv=true`.
- Active limiter: `downlink_backpressure` toggled `pause_edges=51` and
  `resume_edges=51` on smoltcp tx-queue pressure
  (`max_tx_queue_bytes=588901`) while app-owned pending stayed `0`.
- Rejected next moves: do not treat this run as stale pool, iperf3, sing-box,
  TUIC auth, TUN drop, QUIC congestion, terminal pending, or close-drain
  evidence.
- Correct behavior: before the next behavior patch, propose and confirm a
  tx-queue pressure cadence change with focused TDD. The patch must reduce
  pause/resume oscillation without allowing unbounded read-ahead.

## 2026-07-06 - Knife14bw clippy caught wide diagnostic formatter arguments

- Stage: Knife14bw reverse-window diagnostic local gate.
- Symptom: `cargo clippy --all-targets --features harness -- -D warnings`
  failed on `format_tcp_reverse_window_diag` with
  `clippy::too_many_arguments` after the first implementation used eight
  parameters.
- Root cause: diagnostic-only helper functions can still trip repo quality
  gates when they mirror log fields directly as positional parameters.
- Correct behavior: collect diagnostic log fields into a small purpose-specific
  struct and keep the formatter interface narrow. This preserves log output,
  makes call sites clearer, and avoids adding local `allow` attributes for a
  simple design issue.

## 2026-07-06 - Cargo test accepts one test filter before harness args

- Stage: Knife14bw focused local test rerun.
- Symptom: `cargo test --lib test_a test_b test_c` failed with
  `unexpected argument` because Cargo accepts only one test name/filter before
  `--`.
- Correct behavior: use one broad filter such as
  `cargo test --lib client_tun::tests::reverse_window_diag`, or run separate
  filtered commands when exact test names are needed.

## 2026-07-05 - Knife14bg VPS acceptance failed after TUN RX drain patch

- Stage: Knife14bg bounded TUN RX drain cadence acceptance for commit
  `356e2d2`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14bg_tun_rx_drain_usclient_suite_20260705_085637.tar.gz`
- Symptom: clean reverse-first P1 reached only `0.315/0.025 Mbit/s` even
  though `.27 -> .77` and `.33 -> .77` direct reverse preflights were healthy.
- Important discriminator: `tun_rx_drain` processed `25` TCP packets with
  zero errors, while `downlink_backpressure`, terminal pending, close-time
  pending, TUN drops, TUN flush failures, `send_slice` zero/errors, and
  clean-window QUIC loss/congestion all stayed zero.
- Active signal: only about `92 KiB` reached mini_vpn on the reverse data
  stream, and the clean attribution was
  `tuic_stream_read_gap+relay_remote_read_gap`.
- Rejected next moves: do not tune TUN RX drain budget, TUN queue length,
  close/reap grace, terminal pending accounting, downlink egress pacing, stale
  TCP pool, iperf3, sing-box availability, or connection pool from this failed
  clean window.
- Correct behavior: after this failure, stop before behavior edits. First add
  behavior-neutral stream-gap instrumentation and a scoped reverse-only A/B that
  can compare drain enabled versus disabled without later forward-window QUIC
  congestion polluting the result.

## 2026-07-04 - Knife14az reverse_sender_backpressured needs exit-target baseline and stream-gap evidence

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14az_tcp_policy_usclient_suite_20260704_224324.tar.gz`
- Symptom: clean reverse-first P1 on commit `f21e782` produced only
  `0.245/0.009 Mbit/s` while local downlink pressure, terminal pending, TUN
  drops, and QUIC loss/congestion were all absent.
- Rejected assumption: a tiny reverse receiver result after the local TCP socket
  policy patch still implies local smoltcp tx-queue drain is the active limiter.
  In this run `send_queue_max=1`, pending was `0`, and the parser attributed the
  probe to `reverse_sender_backpressured`.
- Follow-up check: `.33 -> .77` direct forward/reverse was healthy
  (`231/232 Mbit/s` receiver), so the suite result was not explained by a
  simple exit-target iperf path bottleneck.
- Correct behavior: when `reverse_sender_backpressured` appears, collect or
  enable exit-target preflight in the suite, then add behavior-neutral TUIC TCP
  stream first-byte/read-gap diagnostics before changing lifecycle, pacing,
  socket buffer, queue, or close/reap behavior again.

## 2026-07-04 - Knife14ay VPS acceptance failed but identified pre-terminal Closed edge

- Stage: Knife14ay local TCP Closed transition diagnostics acceptance for
  commit `67a8c46`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ay_tcp_lifecycle_usclient_suite_20260704_222042.tar.gz`
- Symptom: clean reverse-first P1 reached only `9.15/8.18 Mbit/s` while the
  direct `.27 -> .77` reverse preflight receiver was about `276 Mbit/s`.
- Important discriminator: the new lifecycle line showed
  `prev_state=Established state=Closed source=dirty_relay pending=0
  terminal_candidate=false`, followed by close-tail terminal pending
  `552440` bytes. Terminal pending was therefore a post-Closed accounting
  outcome, not the hidden pre-close loss point.
- Rejected next moves: do not change close/reap behavior, stale pool, iperf3,
  sing-box, TUN queue length, connection pool, or egress pacer for this clean
  reverse-first failure without new evidence.
- Future behavior: before the next behavior patch, confirm a plan that targets
  local smoltcp TCP send policy and tx-queue/receive-window drain. The first
  candidates are focused tests plus local-link socket policy evaluation
  (`TCP_NODELAY` via `set_nagle_enabled(false)` and, if justified,
  `set_ack_delay(None)`).

## 2026-07-04 - Broad cargo fmt check exposed pre-existing repo formatting drift

- Stage: Knife14ay local regression.
- Symptom: `cargo fmt --all -- --check` produced large diffs in many files
  outside the Knife14ay change set, including unrelated modules such as
  `src/main.rs` and `src/reality_upstream.rs`.
- Root cause: repository-wide rustfmt drift predates this diagnostic patch.
  Applying broad formatting here would mix unrelated churn into a lifecycle
  observability commit.
- Correct behavior: do not treat this broad diff as a Knife14ay code
  regression. Keep the current stage scoped, use `git diff --check` for
  whitespace safety, and reserve full-repo rustfmt cleanup for a separate
  coherent task.

## 2026-07-04 - Knife14ax VPS acceptance failed after tx-queue backpressure patch

- Stage: Knife14ax tx-queue-aware downlink backpressure acceptance for commit
  `58f847d`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ax_tx_queue_backpressure_usclient_suite_20260704_215053.tar.gz`
- Symptom: clean reverse-first P1 failed with `iperf3: unable to receive
  results` instead of producing a low but complete `10-20 Mbit/s` result.
- Important discriminator: the new tx-queue pressure metrics did not trigger:
  `downlink_backpressure pause_edges=0`, `max_tx_queue_bytes=0`, and
  `max_pressure_bytes=0`. The clean window also had `global_rx_pressure=0`,
  `tun_tx_dropped_delta=0`, TUN flush failures zero, and QUIC loss/congestion
  delta zero.
- Close-boundary signal: the reverse data socket had only about `221884`
  remote-to-local bytes before `dead_slot_reap` observed
  `tcp_state=Closed active=false can_send=false`, with `41208` bytes classified
  as terminal pending.
- Rejected next moves: do not keep tuning tx-queue backpressure, stale pool,
  iperf3, sing-box, egress pacing, TUN queue length, or QUIC congestion for the
  clean reverse-first root without new evidence.
- Future behavior: after a failed repair run like this, stop before behavior
  edits. First add lifecycle/state-transition diagnostics and focused tests that
  explain why the local TCP socket becomes `Closed` while remote downlink volume
  is still tiny.

## 2026-07-04 - Local cargo test QUIC bind checks fail inside restricted sandbox

- Stage: Knife14ax local regression.
- Symptom: `cargo test` and `cargo test --features harness` failed only for
  `quic::tests::client_endpoint_binds` and
  `quic::tests::client_endpoint_binds_with_each_cc` when run inside the managed
  sandbox. The same filtered tests passed immediately outside the sandbox.
- Root cause: the tests create local QUIC endpoints and require local bind
  permissions not available in the restricted sandbox.
- Correct behavior: when these exact tests fail under sandboxing, rerun the
  cargo test command outside the sandbox before treating it as a code
  regression. Do not change QUIC code or test assertions for this failure mode.

## 2026-07-04 - Knife14aw diagnostics passed but reverse-first throughput stayed low

- Stage: Knife14aw send-window/global_rx diagnostics acceptance for commit
  `ef7364c`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14aw_send_window_globalrx_usclient_suite_20260704_204329.tar.gz`
- Symptom: clean reverse-first P1 reached only `16.4/15.0 Mbit/s`, while
  post-suite direct baselines on `.27 <-> .77` and `.33 <-> .77` were about
  `196-198 Mbit/s`.
- Important discriminator: the new diagnostics showed `send_queue_max=1048576`
  with `send_capacity_min=max=1048576`, `pending_total=0`,
  `global_rx_queue_used_max=258/1024`, no clean-window QUIC loss/congestion,
  and no `send_slice` zero/error. The low throughput is not explained by a
  missing parser tail, relay channel saturation, or a shrunken smoltcp capacity.
- Rejected next moves: do not return to stale pool, iperf3, sing-box, egress
  pacer, TUN queue length, or close/reap tuning without new evidence.
- Future behavior: make downlink backpressure aware of smoltcp tx queue
  occupancy, because app-owned `downlink_pending` can be zero while the socket
  tx queue is saturated and TUN egress drops.

## 2026-07-04 - Knife14av post-iperf reporting passed but VPS throughput failed

- Stage: Knife14av post-iperf close-tail reporting acceptance for commit
  `b7e10b1`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14av_post_iperf_close_tail_usclient_suite_20260704_202011.tar.gz`
- Symptom: reverse-first P1 reached only `11.4/10.1 Mbit/s`, and later reverse
  windows stayed low (`18.1/17.0 Mbit/s`, `24.1/23.3 Mbit/s`).
- Important discriminator: the parser fix worked. The clean reverse-first
  `tcp-handle-close ... pending=69769 ... terminal_pending_reap_bytes=69769`
  line appeared in the same probe summary as `terminal_pending_reap` and
  `pending_at_close`.
- Rejected next moves: do not treat this as a stale-pool, iperf3, sing-box,
  egress-pacer, TUN queue, or clean-window QUIC loss/congestion issue without
  new evidence.
- Future behavior: add send-window and relay queue diagnostics before changing
  close/reap behavior. `global_rx_pressure=0` and `no_send_capacity=N` are not
  detailed enough to distinguish channel backlog, smoltcp tx-buffer fullness,
  terminal `may_send=false`, or receive-window behavior.

## 2026-07-04 - Knife14at first VPS attempt missed local TUIC environment

- Failed report:
  `/tmp/conn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_185107.md`
- Failed bundle:
  `/tmp/conn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_185107.tar.gz`
- Symptom: the first `.27` Knife14at suite attempt stopped before sudo, build,
  tunnel startup, or iperf traffic because the non-login SSH shell had not
  loaded required TUIC environment variables.
- Rejected assumption: if `.env` exists on `.27`, a direct remote suite command
  will automatically load it.
- Correct behavior: before running the suite, check that the required variable
  names exist without printing values, then explicitly source the VPS-local
  `.env` in the same TTY command:
  `set -a && . ./.env && set +a && ...`.
- Security rule: keep `.env` local to the VPS/user environment. Do not print,
  commit, copy, summarize, or store secret values in reports, docs, learning
  memory, commands, or final messages.
- Future debugging rule: if a suite fails at TUIC Env Checks, treat it as an
  invocation/environment failure. Do not analyze mini_vpn, sing-box, iperf, TUN,
  or QUIC behavior until the environment is loaded and the tunnel actually
  starts.

## 2026-07-04 - Non-TTY sudo preflight failure produced no data-plane evidence

- Log bundle:
  `/tmp/conn/mvpn_knife14as_terminal_pending_reversefirst_usclient_suite_20260704_181027.tar.gz`
- Symptom: the first `.27` Knife14as suite attempt stopped at `sudo -v` before
  build, tunnel startup, or iperf traffic. There was no mini_vpn behavior to
  analyze.
- Rejected assumption: a suite that may invoke `sudo -v` can be rerun through a
  non-interactive SSH command and still produce useful acceptance evidence.
- Correct behavior: start `.27` suites that need sudo in a real writable TTY and
  enter the sudo password only at the prompt.
- Security rule: do not store sudo passwords in `.env`, commands, scripts,
  repository files, docs, learning memory, reports, or summaries.
- Future debugging rule: if a suite fails before build or service preflight,
  treat it as a harness/operator failure, fix the invocation mode, and do not
  spend analysis budget on TUIC, QUIC, TUN, smoltcp, or iperf.

## 2026-07-04 — Knife14ar close-safe egress pacing failed VPS acceptance

- Stage: Knife14ar close-safe downlink egress pacing acceptance for commit
  `3baf476`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ar_close_safe_default_usclient_suite_20260704_164651.tar.gz`
- Symptom: clean reverse-first P1 reached only `17.2/16.2 Mbit/s`, still below
  the Knife14ap clean reverse-first receiver result of `22.0 Mbit/s`.
- Important discriminator: the intended defaults and code path were active
  (`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`,
  `tun_flush_deferred=0`). Direct reverse baselines were healthy
  (`.27 -> .77` receiver `271 Mbit/s`, `.33 -> .77` receiver
  `285 Mbit/s`). The clean reverse window had no QUIC loss/congestion delta, no
  TUN drops, no `send_slice` zero/error, and no TUN flush syscall failure.
- Close-boundary signal: after the clean reverse window, a relay still hit
  `reason=dead_slot_reap` with `pending=224765`, `pending_high=583624`,
  `tcp_state=Closed`, `active=false`, `can_send=false`, and
  `tun_flush_deferred=0`.
- Future behavior: do not keep tuning downlink egress pacing for this failure.
  The next code plan must add deterministic close/reap tests and explicit
  accounting for terminal pending bytes before another VPS throughput run.

## 2026-07-04 — Knife14aq egress pacing default failed VPS acceptance

- Stage: Knife14aq default downlink egress pacing acceptance for commit
  `212ce26`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14aq_egress_pacing_default_usclient_suite_20260704_160317.tar.gz`
- Symptom: clean reverse-first P1 reached only `16.3/13.8 Mbit/s` with
  `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=65536`, worse than the previous
  Knife14ap clean reverse-first receiver result of `22.0 Mbit/s`.
- Important discriminator: direct `.27 -> .77` and `.33 -> .77` reverse
  baselines were healthy (`295 Mbit/s` and `283 Mbit/s` receiver). The clean
  reverse window had no QUIC loss/congestion delta, no `send_slice` zero/error,
  and no TUN flush syscall failure. The pacer did engage
  (`tun_flush_deferred=1057`) and reduced TUN drops (`366 -> 65`), but
  backpressure remained and throughput fell.
- Close-boundary signal: the same run reaped a relay with
  `reason=dead_slot_reap`, `pending=576827`, `tcp_state=Closed`,
  `can_send=false`, and `can_recv=false`.
- Future behavior: do not continue tuning this default as if it were accepted.
  Before further VPS runs, change the design so remote-payload egress pacing is
  close-safe or default-off, and add tests for pending downlink bytes across
  close/reap boundaries.

## 2026-07-04 — Codex interactive sudo over SSH needs a writable PTY session

- Stage: Knife14ap VPS acceptance.
- Failed/non-useful attempts:
  - `ssh -tt ...` without `tty=true` reached `.27` `sudo -v`, but the Codex
    session did not keep writable stdin for the password prompt.
  - Non-TTY suite invocation failed at `sudo -v` even after an outer
    `sudo -n true` check, because the script uses a naked `sudo -v` and sudo
    needed a terminal/password for that invocation.
- Working behavior: run the suite over SSH with a true writable PTY
  (`exec_command` with `tty=true`) and enter the sudo password only into the
  prompt. Do not place the password in scripts, repository files, reports, or
  final summaries.
- Future behavior: for `.27` acceptance runs where sudo may be cold, start the
  VPS suite in a writable PTY from the beginning. A plain `ssh -tt` subprocess
  can still leave Codex unable to answer the prompt.

## 2026-07-04 — Knife14ao default-watermark acceptance did not reproduce the high A/B result

- Stage: Knife14ao default downlink-watermark acceptance for commit `0d0765f`.
- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14ao_default_bp512_128_flush256_tty_usclient_suite_20260704_144755.tar.gz`
- Symptom: `.27` ran the current branch with the new product defaults active
  (`high=524288B`, `low=131072B`, `flush=262144B`), but clean reverse-first P1
  reached only `22.5/21.4 Mbit/s`. The earlier no-code 512/128 KiB A/B had
  reached `158/157 Mbit/s`.
- Important discriminator: direct `.27 -> .77` and `.33 -> .77` iperf baselines
  were healthy, sing-box and iperf3 services were active, and the clean
  reverse-first tunnel window had no QUIC loss/congestion or inherited
  congestion. The remaining signals were local:
  `local_tun_egress_drop+local_downlink_backpressure`,
  `max_pending_bytes=589698`, and `tun_tx_dropped_delta=496`.
- Future behavior: do not treat 512/128 KiB watermarks as a final throughput
  fix. Before further tuning or scheduler changes, add downlink flush progress
  observability so the next bundle can distinguish `can_send=false`, zero/short
  `send_slice`, budget clipping, and TUN/qdisc loss after successful smoltcp
  acceptance.

## 2026-07-04 — Manual `.27` suite invocations must explicitly source `.env`

- Stage: Knife14ao downlink-watermark A/B.
- Failed bundle:
  `/tmp/conn/mvpn_knife14ao_bp256_64_flush256_defaultqlen_tty_usclient_suite_20260704_142904.tar.gz`
- Symptom: the suite stopped before startup with all TUIC environment variables
  reported missing.
- Important discriminator: `/home/ubuntu/mini_vpn/.env` existed, had mode 600,
  and `set -a; . ./.env; set +a` exported the required TUIC variables. The file
  uses `export KEY=...`, so a naive `grep '^KEY='` check is not a valid
  presence test.
- Correct behavior: when invoking the suite manually over SSH, run it from
  `/home/ubuntu/mini_vpn` after sourcing `.env` in the same shell. Keep secrets
  out of command output, scripts, repository files, reports, and learning
  memory.

## 2026-07-04 — Knife14ao A/B #2 failed before TUIC app-level logging

- Stage: Knife14ao downlink-watermark A/B with
  `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=262144`,
  `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=65536`, and
  `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`.
- Failed bundle:
  `/tmp/conn/mvpn_knife14ao_bp256_64_flush256_defaultqlen_tty_usclient_suite_20260704_143108.tar.gz`
- Symptom: `.27` loaded the TUIC env and direct baselines were healthy
  (`.27 -> .77` reverse 290 Mbit/s receiver, `.33 -> .77` reverse
  287 Mbit/s receiver), but `client-tun` exited during startup with
  `tuic auth finish: sending stopped by peer: error 0`.
- Important discriminator: `.33` `sing-box` was active, had not restarted, and
  UDP `:8443` was owned by the `sing-box` process, but
  `/var/log/sing-box.log` mtime remained at `2026-07-04 14:27:09 +0800`.
  The failed 14:31 startup did not produce a TUIC inbound log line.
- Future behavior: do not interpret this failure as a throughput result or a
  watermark regression. Treat it as a startup/QUIC-handshake boundary issue
  until a rerun reaches the iperf probe window or server-side logs show an
  app-level TUIC connection.

## 2026-07-04 — `.27` suite needs a TTY when sudo timestamp is cold

- Stage: Knife14an VPS acceptance.
- Failed bundle:
  `/tmp/conn/mvpn_knife14an_flush256_defaultqlen_usclient_suite_20260704_140940.tar.gz`
- Symptom: the suite stopped at `sudo -v` with
  "a terminal is required to read the password" before building or starting
  `client-tun`.
- Important discriminator: rerunning the same suite through `ssh -tt` and
  entering the `.27` sudo password succeeded. The successful bundle was
  `/tmp/conn/mvpn_knife14an_flush256_defaultqlen_tty_usclient_suite_20260704_141008.tar.gz`.
- Future behavior: when `.27` sudo timestamp may be cold, use an interactive
  TTY for the suite instead of non-TTY SSH. Do not put the sudo password into
  scripts, repo files, reports, or learning memory.

## 2026-07-04 — Use `.27` HTTPS origin or known_hosts before GitHub SSH pull

- Stage: Knife14an VPS checkout sync.
- Command failed on `.27`: one-shot `git pull --ff-only` from
  `git@github.com:S7245/mini_vpn.git`.
- Error: `Host key verification failed`.
- Working fallback: `.27` already had
  `origin=https://github.com/S7245/mini_vpn.git`, and
  `git pull --ff-only origin codex/knife14d-downlink-reap-open` succeeded.
- Future behavior: do not use GitHub SSH from `.27` until its known_hosts entry
  is deliberately initialized. HTTPS origin is fine for fetch/pull; local Mac
  still needs SSH for push.

## 2026-07-04 — Full `cargo fmt --check` reports broad pre-existing formatting drift

- Stage: Knife14an local verification after adding the downlink flush budget.
- Command failed locally: `cargo fmt --check`.
- Symptom: rustfmt wanted to rewrite large portions of `src/client_tun.rs`,
  `src/reality_upstream.rs`, and `src/main.rs`, including many lines outside
  the Knife14an diff.
- Important discriminator: focused behavior checks passed:
  `cargo test --lib client_tun`, `bash -n scripts/knife14b-usclient-tunnel-suite.sh`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`, and
  `git diff --check`.
- Correct behavior: do not run whole-repo `cargo fmt` inside a scoped
  throughput fix because it would create unrelated churn and make review harder.
  Use `git diff --check` for patch whitespace, and make formatting cleanup a
  separate explicit task if the project decides to normalize rustfmt output.

## 2026-07-04 — Knife14al cleanup regex killed the active suite shell

- Failed bundle:
  `/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen_usclient_suite_20260704_132521.tar.gz`
- Symptom: the default-queue same-window diagnostic stopped during
  `Stop Old Tunnel`, before any tunnel iperf connection reached `.77`.
- Root cause: `pgrep/pkill -f '[m]ini_vpn.*client-tun'` matched the active
  shell/SSH command line because it included the `/home/ubuntu/mini_vpn` working
  path and the `knife14b-usclient-tunnel-suite.sh` script name. The cleanup
  therefore killed the runner instead of only stale `mini_vpn client-tun`
  processes.
- Fix: commit `1ab58e3` matches `comm == mini_vpn` plus an independent
  `client-tun` argv token, and adds `--self-test` false-positive coverage.
- Future behavior: before adding `pkill -f` to a harness, write or run a
  self-test with realistic SSH/sudo/script command lines. Prefer PID lists from
  `ps` parsing over broad command-line regexes for destructive cleanup.

## 2026-07-04 — Knife14al attribution missed inherited QUIC congestion

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`
- Symptom: the standard P1 reverse probe after a high-throughput forward probe
  reported `attribution: no_pressure_signal` while throughput was only
  11.5 Mbit/s sender and 10.7 Mbit/s receiver.
- Important discriminator: the reverse window did not accumulate new QUIC
  loss/congestion deltas, but it started with a damaged connection state:
  `cwnd=5808`, `lost_bytes=500976100`, and `congestion_events=93571` inherited
  from the preceding forward burst.
- Correct behavior: do not interpret this post-forward reverse sample as a
  clean no-pressure branch. Prefer reverse-first/fresh-connection samples for
  directional diagnosis, or reset the TUIC connection before the comparison.
- Future behavior: update low-RTT attribution to flag absolute low cwnd and
  preexisting high loss/congestion counters, not only per-window deltas.

## 2026-07-04 — Knife14ak qlen=5000 sample hit reverse sender backpressure, not TUN drops

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ak_tunqlen5000_usclient_suite_20260704_131103.tar.gz`
- Tested commit: `b31b234`
- Setup: `.27` sourced `/home/ubuntu/mini_vpn/.evn` quietly, used
  `TUN_TX_QUEUE_LEN=5000`, `MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`,
  and `RUN_REVERSE_FIRST_P1=1`.
- The qlen setup itself worked: report showed `tun_tx_queue_len_actual=5000`.
- Symptom: reverse tunnel P1 collapsed to 0.314 Mbit/s sender and 0.057 Mbit/s
  receiver, while direct client-target and exit-target reverse preflights were
  healthy.
- Important discriminator: `tun_tx_dropped_delta=0`, downlink backpressure was
  zero, local/global pressure was zero, QUIC loss/congestion and blocked-frame
  deltas were zero, and the close tail had `pending=0`.
- Exit-side clue: sing-box logged the TUIC inbound/direct outbound opens and
  later `connection download closed: stream 4 canceled by remote with error
  code 0` for the probe window.
- Correct behavior: do not interpret this as proof that qlen=5000 fixes or
  breaks mini_vpn throughput. The run switched to the
  `reverse_sender_backpressured` branch. Before product changes, add or run
  paired diagnostics that capture target sender, exit sing-box, and mini_vpn
  TUIC receive evidence during the same reverse probe.

## 2026-07-04 — Use portable find commands on macOS

- Command failed locally after extracting the Knife14ak bundle:
  `find /tmp/mini_vpn/knife14ak_tunqlen5000_131103 -maxdepth 1 -type f -printf '%f\n'`.
- Error: macOS/BSD `find` does not support GNU `-printf`.
- Working fallback: `find <dir> -maxdepth 1 -type f -exec basename {} \; | sort`.
- Future behavior: avoid GNU-only `find -printf` in this Mac workspace unless
  GNU find is explicitly installed.

## 2026-07-04 — HTTPS origin push failed; SSH one-shot push worked

- Command failed: `git push` against `origin=https://github.com/S7245/mini_vpn.git`.
- Error: `fatal: could not read Username for 'https://github.com': Device not configured`.
- Working fallback: `git push git@github.com:S7245/mini_vpn.git codex/knife14d-downlink-reap-open`.
- Future behavior: do not assume HTTPS origin can push from this session. If
  SSH GitHub access is available, use a one-shot SSH URL or ask before changing
  `origin`.

## 2026-07-04 — Source `.evn` with output redirected; it can dump secrets

- First VPS suite invocation failed because TUIC env vars were not loaded:
  `/tmp/mini_vpn/mvpn_knife14aj_tun_drop_usclient_suite_20260704_111955.tar.gz`.
- The `.27` project root had `.evn`, not `.env`, and it was missing explicit
  `MINI_VPN_TUIC_SNI`/`MINI_VPN_TUIC_ALPN`; repo convention fills those as
  `example.com` and `h3`.
- Pitfall: sourcing `.evn` printed exported environment lines to stdout in the
  SSH command stream, including sensitive values. The generated suite bundle
  still redacted secrets, but the command stream did not.
- Future behavior: never source `.evn` directly in a command whose stdout is
  captured. Use a quiet wrapper such as `set -a; source ./.evn >/dev/null
  2>&1; set +a`, then explicitly export non-secret defaults.

## 2026-07-04 — Knife14aj initial plan mistook Closed pending for drainable pressure

- Initial design branch: add a progress-sensitive grace for inactive pending
  even when `can_send=false`, based on the post-restart close tail
  `dead_slot_reap ... pending=2111595 ... tcp_state=Closed active=false
  can_send=false`.
- Re-grounding result: in smoltcp `0.10.0`, `can_send()` is false for `Closed`
  because `may_send()` only permits `Established` and `CloseWait`. A grace in
  this branch cannot flush userspace pending into the local TCP socket.
- Correct behavior: do not weaken knife14u's immediate reap for
  `Closed && !can_send` pending. Attribute earlier pressure instead, especially
  TUN/qdisc TX drops that were visible in the VPS report but absent from the
  per-probe summary.
- Command pitfall encountered: `cargo test` accepts one filter; running
  `cargo test --lib test_a test_b` fails with "unexpected argument". Use a
  shared substring such as `cargo test --lib reap_predicate` for grouped tests.

## 2026-07-04 — Knife14ai post-restart reverse P1 still reaps closed relay with pending downlink

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_after_singbox_restart_usclient_suite_20260704_091224.tar.gz`
- Tested commit: `34d5cb7`
- Precondition: pool=4 startup-only smoke failed before restarting `.33`
  sing-box, then passed immediately after `.33` restart at 09:11:13 CST.
- Symptom: reverse-first P1 completed, but throughput remained bursty and low
  relative to direct baselines: iperf sender 24.8 Mbit/s, receiver 23.6 Mbit/s.
- Important discriminator: attribution reported `local_downlink_backpressure`
  with two pause/resume edges and max pending 2,149,256 bytes. QUIC had no
  loss/congestion/blocked deltas; local write and global_rx pressure counters
  were zero.
- Close-tail signal: handle 1 closed by `dead_slot_reap` with
  `state=Relaying pending=2111595`, `tcp_state=Closed active=false
  can_send=false can_recv=false`, no send-slice errors, and no TUN flush
  failures. Standard P1/full sweep were skipped because no fresh quiet metrics
  tick arrived within the short gate.
- Correct behavior: do not continue stale TCP pool slot diagnosis and do not
  change iperf3. The next repair branch should target downlink pending lifecycle
  and TUN local delivery/backpressure for inactive or closed TCP handles.

## 2026-07-03 — Knife14ai pool=4 failed before tunnel iperf at TUIC auth finish

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_usclient_suite_20260703_234304.tar.gz`
- Tested commit: `34d5cb7`
- Symptom: the pool=4 experiment failed during `client-tun` startup with
  `tuic auth finish: sending stopped by peer: error 0`. The suite never reached
  tunnel iperf.
- Important discriminator: `.77` iperf3 stayed active and completed the
  direct/exit-target preflights at roughly 285-309 Mbit/s during the same run.
  The preceding pool=1 control also completed tunnel iperf, so credentials and
  the basic TUIC path were not globally broken.
- Exit-side signal: `.33` sing-box was systemd-active. Its log window showed
  pool=1 TUIC inbound/direct outbound opens at 23:42:07 CST and one stream
  cancel at 23:42:38 CST, but no corresponding pool=4 TUIC inbound line around
  the 23:43 startup failure.
- Correct behavior: do not adjust iperf3 or change Rust data-plane throughput
  code from this failure. First validate `MINI_VPN_TUIC_TCP_POOL>1` with a
  startup-only smoke and collect `.33` TUIC logs; if auth failure persists,
  treat sing-box service state or multi-connection startup behavior as the
  next hypothesis before running another expensive throughput suite.

## 2026-07-03 — Knife14ah reverse-first backpressured target sender through TUIC

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ah_usclient_suite_20260703_231523.tar.gz`
- Tested commit: `4282682`
- Symptom: reverse-first P1 over the tunnel exited 0 but delivered only
  88.2 KBytes / 24.1 Kbit/s to the receiver. The suite skipped standard P1 and
  full sweep because one relay stayed active after the quiet wait.
- Important discriminator: client-side relay diagnostics showed no local write
  pressure, no global_rx pressure, no downlink backpressure, no QUIC
  loss/congestion or blocked-frame deltas, and no stale-pool reconnects.
  The relay accepted 186,936 remote bytes total into smoltcp with `pending=0`
  and zero TUN flush failures.
- Late signal: 90,312 bytes arrived before local `Finish`; another 96,624
  bytes arrived after local `Finish`, then the relay closed by
  `half_closed_idle_timeout`.
- Server-side signal: `.77` iperf3 journal showed the reverse sender only sent
  3.00 MBytes in the first second, then 29 seconds of 0 bytes with cwnd about
  432 KBytes. `.33` sing-box INFO logs showed TUIC inbound and direct outbound
  opens to `.77:5201` at the run time, but no close/error detail.
- Correct behavior: do not tune client local downlink queues, socket buffers,
  stale slot handling, or congestion control from this result. First add or
  collect diagnostics that distinguish sing-box/TUIC server-side stream
  flow-control/write pressure, target TCP receive-window behavior, and TUIC
  Connect half-close semantics.

## 2026-07-03 — Full `cargo test` can fail on local QUIC endpoint bind

- Stage: Knife14ah local verification.
- Symptom: full `cargo test` passed 251 tests but failed
  `quic::tests::client_endpoint_binds` and
  `quic::tests::client_endpoint_binds_with_each_cc`.
- Important discriminator: `src/quic.rs` had no diff in this stage, and focused
  relay/parser tests passed. The failing tests exercise `client_endpoint`, which
  binds `0.0.0.0:0` and creates a Quinn endpoint through the local async runtime.
- Correct behavior: treat these two tests as local endpoint-bind environment
  checks, not evidence about relay late-remote diagnostics. For scoped data-plane
  observability stages, report the full-test residual risk and rely on focused
  Rust tests plus VPS acceptance for QUIC path behavior unless the stage touches
  `src/quic.rs`.

## 2026-07-03 — Knife14ag reverse-first failed with no pressure signal

- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ag_usclient_suite_20260703_225103.tar.gz`
- Tested commit: `fbe030c`
- Symptom: reverse-first P1 over the tunnel exited 0 but delivered
  0 receiver bytes. The suite then skipped standard P1/full sweep because the
  tunnel still had one active relay after the quiet wait.
- Important discriminator: the attribution summary reported no local write
  pressure, no global_rx pressure, no downlink backpressure, no QUIC
  loss/congestion delta, no blocked-frame delta, and no stale reconnects.
  During the iperf window the data relay had `remote_to_global_rx_bytes=0`.
- Late signal: after iperf finished and `tcp-relay-write-half-closed
  reason=local_finish` fired, the same relay later read 43,772 remote bytes and
  closed by `half_closed_idle_timeout`.
- Correct behavior: do not continue stale-slot diagnosis and do not tune local
  pressure/backpressure or QUIC congestion parameters from this result. First
  add/collect diagnostics that distinguish TUIC control vs data stream timing,
  server-side target connect/write timing, and late remote bytes after local
  half-close.

## 2026-07-03 — Source `.evn` silently, never echo secret exports

- Stage: Knife14ag VPS suite launch after the user created `.evn` in `.27`'s
  project root.
- Symptom: sourcing `.evn` in a noninteractive command produced export output,
  which exposed secret values in command output.
- Correct behavior: when loading project-local env files on VPS hosts, use
  `set -a && . ./.evn >/dev/null && set +a` and only run secret-safe
  presence/length checks. Do not print env-file output, TUIC UUIDs, passwords,
  private keys, or raw env dumps into reports, learnings, or final summaries.

## 2026-07-03 — Run `.27` suite as root when `sudo -v` cannot prompt

- Stage: Knife14ag VPS suite launch from noninteractive SSH.
- Symptom: invoking the suite as `ubuntu` failed early at `sudo -v` because
  the session had no TTY/password prompt, even though `sudo -n true` was
  available.
- Correct behavior: for noninteractive agent-launched `.27` suites, load the
  env file silently and invoke the suite itself with
  `sudo -n -E env ... bash scripts/knife14b-usclient-tunnel-suite.sh`. That
  makes the suite's internal `sudo -v` run as root and keeps env propagation
  explicit.

## 2026-07-03 — Noninteractive Client VPS SSH does not carry TUIC env

- Stage: Knife14ag VPS acceptance attempt after pushing `4c62d74`.
- Preflight result: `.33` sing-box and `.77` iperf3 were active; `.27` was
  fast-forwarded to `4c62d74`; `bash scripts/knife14b-lowrtt-probe.sh
  --self-test` passed on `.27`.
- Blocker: noninteractive SSH, `bash -lc`, and `sudo -E` on `.27` all reported
  `MINI_VPN_TUIC_SERVER`, `MINI_VPN_TUIC_UUID`, `MINI_VPN_TUIC_PASSWORD`,
  `MINI_VPN_TUIC_SNI`, `MINI_VPN_TUIC_CA_PATH`, and `MINI_VPN_TUIC_ALPN` as
  missing. A file-name-only search found no common `.env`/`*env*` candidate.
- Correct behavior: before launching an expensive VPS suite from a fresh SSH
  session, verify TUIC env presence without printing values. If missing, ask for
  an env file path or have the user run/export the credentials from a shell that
  already has them; do not inspect shell history or reports for secrets.

## 2026-07-03 — Shell parser tests must cover Bash and BSD awk strictness

- Stage: Knife14ag local verification for low-RTT probe attribution summaries.
- Symptoms:
  - `bash -n scripts/knife14b-lowrtt-probe.sh` failed when self-test used one
    multi-line `case` pattern with adjacent `*pattern*` fragments.
  - `bash scripts/knife14b-lowrtt-probe.sh --self-test` failed on macOS awk with
    `illegal primary in regular expression +local_write_pressure+`.
  - A later self-test printed success but exited non-zero because an `EXIT` trap
    referenced a local `tmpdir` after the function returned under `set -u`.
- Root causes:
  - Bash `case` patterns cannot be composed that way across lines.
  - awk string `!~` and `split(..., "+")` treat the right side/separator as a
    regex; bare or leading `+` is not portable.
  - `EXIT` traps run after local variables leave scope.
- Correct behavior: use explicit `assert_contains` checks, compare de-duplicated
  values with split/string loops rather than regex membership, use `[+]` for a
  literal plus separator, and either expand temp paths into traps or clear traps
  before returning.

## 2026-07-03 — TUN discovery awk regex must not double-escape `/`

- Symptom: `knife14af2` printed `awk: syntax error` while discovering `tun0`,
  but continued through the fallback path and completed the suite.
- Root cause: an awk program inside single quotes used `\\//`; awk received an
  extra backslash before `/`.
- Correct behavior: use `\//` inside the single-quoted awk regex, matching the
  earlier stale-TUN cleanup code.

## 2026-07-03 — VPS `/tmp` mode can break acceptance before code runs

- Symptom: the first `knife14af` attempt on `.27` failed before build/suite with
  `mkdir: cannot create directory '/tmp': Permission denied`.
- Root cause: `.27` had `/tmp` as `drwx------ root root` instead of the standard
  sticky world-writable mode.
- Correct behavior: repair the host with `sudo chmod 1777 /tmp`, then create and
  chown the suite output directory before rerunning.

## 2026-07-03 — Use cargo fmt for file-level Rust formatting in this 2024-edition repo

- Symptom: running bare `rustfmt src/tuic.rs` failed with Rust 2015 parsing
  errors (`async fn` not permitted, let chains require Rust 2024).
- Root cause: bare `rustfmt` did not pick up the crate's `edition = "2024"`.
- Correct behavior: use `cargo fmt -- <path>` for targeted formatting, or expect
  `cargo fmt --check` to report historical repository-wide formatting noise.

## 2026-07-03 — A TCP pool slot can be stale before `close_reason` is observed

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ae3_usclient_suite_20260703_212412.tar.gz`
- Tested commit: `ebd3567`
- Symptom: the final full reverse P1 opened TUIC TCP on `conn=1`, then the
  connection immediately reported `closed=TimedOut`; iperf reverse transferred
  0 bytes and exited after the 50s wrapper timeout.
- Rejected assumption: `close_reason().is_none()` before `open_bi` is enough to
  prove a pooled QUIC connection is safe for a new TCP relay.
- Correct behavior: track TCP pool slot freshness and reconnect stale slots
  before opening a new TUIC Connect stream, so a relay does not inherit a
  connection that only reveals timeout after the stream is handed to the pump.

## 2026-07-03 — Stale pool reconnect must be idle-only

- Symptom avoided during code review: a simple stale timeout on a shared QUIC
  pool slot would close the whole connection even if another TCP relay on that
  slot was still active, breaking long-lived or concurrent TCP flows.
- Correct behavior: only reconnect a stale non-primary pool slot when its active
  or opening relay count is zero; keep a CAS-reserved lease from slot selection
  through returned stream drop, and let only the exclusive idle owner reconnect.

## 2026-07-03 — Active sing-box can still fail TUIC auth until restarted

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ae2_usclient_suite_20260703_212154.tar.gz`
- Tested commit: `ebd3567`
- Symptom: `.33` sing-box was `active`, config credentials matched the client,
  and direct `.33 <-> .77` iperf was healthy, but mini_vpn startup failed at
  `tuic auth finish: sending stopped by peer: error 0`.
- Observed fix: restarting `sing-box` on `.33` made the next run authenticate
  and proceed into tunnel throughput testing.
- Correct behavior: when TUIC auth fails despite matching config, treat VPS
  service state as a first-class branch; collect `.33` logs/status and restart
  the service before changing mini_vpn protocol code.

## 2026-07-03 — Exit SSH preflight must not depend on root known_hosts

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ae_usclient_suite_20260703_211833.tar.gz`
- Tested commit: `70e59a0`
- Symptom: `EXIT_TO_TARGET_IPERF_CHECK=1` failed before tunnel testing with
  `Host key verification failed` while SSHing from `.27` to `.33`.
- Root cause: the suite runs under `sudo -E`, so the SSH command used root's
  host-key store rather than `ubuntu`'s interactive `known_hosts`; with
  `BatchMode=yes`, first-use host-key confirmation cannot be answered.
- Correct behavior: Exit SSH preflight should be explicitly noninteractive by
  default, using a suite-local known-hosts file and `StrictHostKeyChecking`
  policy that accepts new hosts while still failing on changed host keys.

## 2026-07-03 — Client↔Target direct baseline is insufficient for tunnel reverse

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ad_usclient_suite_20260703_183503.tar.gz`
- Tested commit: `33659c9`
- Symptom: direct `.77 -> .27` reverse reached 268 Mbit/s, but tunnel reverse
  stayed around 15-22 Mbit/s. The new diagnostics showed reverse data did reach
  the client (`remote_to_global_rx_bytes` grew to tens of MB), so the failure was
  no longer the old "no server bytes at all" branch.
- Rejected assumption: a healthy Client↔Target direct reverse baseline proves
  the reverse path used by the tunnel is healthy.
- Correct behavior: also measure Exit↔Target from `.33`, especially `.77 -> .33`
  via `iperf3 -R`, because tunnel reverse is Target→Exit→TUIC→Client. Without
  this baseline, server/path attribution and client data-plane attribution are
  mixed.

## 2026-07-03 — `cargo fmt --check` is noisy on the current historical tree

- Stage: Knife14ad local verification after adding stream-level diagnostics.
- Symptom: `cargo fmt --check` failed with a huge rustfmt diff spanning many
  pre-existing files and hunks outside the current change. No files were changed
  because it was a check-only command.
- Rejected assumption: a failing full-repo fmt check necessarily means the
  current small patch should run `cargo fmt` and accept the resulting churn.
- Correct behavior: for scoped acceptance/debugging stages, use
  `git diff --check`, focused tests, full `cargo test`, and `cargo clippy
  --all-targets -- -D warnings`. Only run/apply broad formatting when the stage
  explicitly owns formatting cleanup.

## 2026-07-03 — Knife14ab disproved socket buffer as sufficient reverse fix

- Log bundle: `/tmp/mini_vpn/mvpn_knife14ab_usclient_suite_20260703_172934.tar.gz`
- Tested commit: `3af4f7c`
- Symptom: 1MiB smoltcp TCP socket buffers were confirmed in the startup log and
  forward throughput improved, but tunnel reverse remained only 17-32 Mbit/s
  while direct reverse was 281 Mbit/s.
- Important discriminator: client-side `send_slice_zero`/`send_slice_errors`
  stayed zero, and reverse throughput was bursty with long zero-throughput
  windows. This is not the same as the pre-knife14ab 64KiB local socket-window
  hypothesis.
- Rejected assumption: after raising smoltcp socket buffers, any remaining
  reverse failure should be fixed by increasing the same buffers again.
- Correct behavior: run a fresh reverse-only P1 probe before any forward pressure
  and test with an explicit TUIC TCP connection pool. If fresh reverse is still
  low, collect `.33`/sing-box or path-level evidence for server-side/downlink
  QUIC behavior.

## 2026-07-03 — Knife14aa showed healthy direct reverse but tiny tunnel reverse

- Log bundle: `/tmp/mini_vpn/mvpn_knife14aa_usclient_suite_20260703_162932.tar.gz`
- Tested commit: `7621bc6`
- Symptom: direct `.77:5201 -R` reached 299 Mbit/s receiver, but tunnel reverse
  stayed at 8.25 Mbit/s standalone P1 and 18-27 Mbit/s in the full sweep.
- Important discriminator: `.77` iperf service and the direct reverse route were
  healthy; QUIC `tx_blocked` stayed zero; client logs showed
  `tcp-downlink-backpressure` and pending downlink at close.
- Rejected assumption: reverse tunnel collapse can still be blamed on `.77`
  service health after direct `iperf3 -R` passes.
- Correct behavior: test whether the 65,535-byte smoltcp TCP tx buffer is the
  local downlink window bottleneck by making socket rx/tx buffers configurable
  and running the high-throughput suite with larger explicit values.
- Future debugging rule: for reverse/downlink bottlenecks, always compare direct
  reverse baseline with tunnel reverse and inspect local socket/window sizing
  before changing QUIC CC or relay FIN logic again.

## 2026-07-03 — Knife14z reverse results lacked a direct reverse baseline

- Log bundle: `/tmp/mvpn_knife14z_usclient_suite_20260703_154429.tar.gz`
- Tested commit: `1b6f2d8`
- Symptom: Cubic and BBR both showed Kbit/s-scale reverse tunnel results, while
  the suite only proved direct forward `.77:5201` iperf was healthy.
- Important discriminator: BBR dramatically improved forward P1/P2/P4, so the
  remaining reverse failure cannot be explained by "Cubic only" or by the CC
  sweep wrapper itself.
- Rejected assumption: a healthy direct forward iperf preflight is enough to
  attribute reverse tunnel collapse to relay/TUIC code.
- Correct behavior: measure direct `iperf3 -R` before the target route is moved
  into the TUN. If the command fails, stop or explicitly mark reverse tunnel
  results as degraded evidence.
- Future debugging rule: every reverse tunnel acceptance bundle needs a direct
  reverse baseline recorded before the tunnel starts.

## 2026-07-03 — Local CC sweep smoke stops at the host guard on macOS

- Command pattern:
  `CC_SWEEP="cubic bbr" OUT_DIR=/tmp/mvpn_cc_sweep_smoke* SUITE_TAG=knife14z_smoke CHECK_VPS_SERVICES=0 BUILD_RELEASE=0 bash scripts/knife14b-usclient-tunnel-suite.sh`
- Symptom: the parent wrapper correctly dispatched the `cubic` child suite, but
  the child failed at `此脚本面向 Ubuntu/Linux Client VPS。当前内核: Darwin`.
- Rejected assumption: a developer-machine smoke run can validate the later
  Ubuntu-only TUIC env checks.
- Correct behavior: on macOS, use the smoke only to validate `CC_SWEEP`
  dispatch, child artifact naming, and parent bundle creation. The missing-env,
  sudo, build, VPS service, and tunnel startup checks must be trusted to the
  Ubuntu/VPS run.

## 2026-07-03 — ee2e83a proved client window tuning alone is insufficient

- Log bundle: `/tmp/mvpn_knife14x_usclient_suite_20260703_120013.tar.gz`
- Tested commit: `ee2e83a`
- Symptom: direct `.77` iperf receiver was healthy at about 285 Mbit/s, but
  tunnel forward remained Kbit/s-to-low-Mbit/s and reverse remained
  Kbit/s-scale or timed out.
- Important discriminator: the startup log confirmed the new client windows
  (`stream_rx=8388608B`, `conn_rx=33554432B`, `send=33554432B`), yet
  `tcp-local-write-pressure` still appeared 104 times with max wait about
  37.8s. `global_rx_pressure_events` stayed zero.
- Rejected assumption: making the client QUIC windows explicit is enough to
  resolve the long `write_all` stalls.
- Correct behavior: add QUIC connection stats around the stall so the next run
  can attribute it to peer flow-control, congestion/path loss, or server/egress
  behavior.
- Future debugging rule: after a client-window change is confirmed in the live
  log, do not repeat that same tuning loop. Instrument blocked frames and path
  stats before deciding between server config, BBR/Cubic A/B, or VPS path tests.

## 2026-07-03 — 95bfe47 exposed long TUIC stream write waits after coalescing

- Log bundle: `/tmp/mvpn_knife14w2_usclient_suite_20260703_112033.tar.gz`
- Tested commit: `95bfe47`
- Symptom: direct `.77` iperf receiver was healthy at about 281 Mbit/s, but
  tunnel forward P1 was 2.83 Mbit/s receiver and reverse P1 was 314 Kbit/s
  receiver. Reverse P2/P4/P8 later timed out.
- Important discriminator: `global_rx_pressure_events=0`; the main loop was
  mostly parked; forward writer payloads were often 64KiB-class; and
  `tcp-local-write-pressure` appeared 110 times with waits up to multi-second
  and tens-of-seconds ranges.
- Rejected assumption: after writer-side coalescing, remaining low throughput is
  still caused by too many MSS-sized `write_all` calls.
- Correct behavior: make client QUIC stream/data/send windows explicit and log
  them. If pressure remains with larger client windows, investigate peer/server
  flow-control, congestion controller choice, and path loss.
- Future debugging rule: when coalescing proves batching is active, do not keep
  tuning relay batching blindly. Follow the pressure signal into QUIC transport
  windows and congestion-control evidence.

## 2026-07-03 — 1144fc2 suite failed because root PATH missed cargo

- Log bundle: `/tmp/mvpn_knife14w_usclient_suite_20260703_111144.tar.gz`
- Tested commit: `1144fc2`
- Symptom: suite stopped at `BUILD_RELEASE=1 但 cargo 不可用。` before VPS
  service preflight, tunnel startup, or iperf traffic.
- Important discriminator: the archive contained only the markdown report; there
  was no accept log evidence to inspect for data-plane behavior.
- Rejected assumption: because the checkout is clean and `BUILD_RELEASE=1` is
  set, root can necessarily run `cargo` through `PATH`.
- Correct behavior: resolve cargo from `CARGO`, `PATH`, `$HOME/.cargo/bin`,
  `/home/ubuntu/.cargo/bin`, or `/root/.cargo/bin`, then record the chosen path
  and version in the report.
- Future debugging rule: if a suite fails before `.33/.77` preflight, fix the
  harness/environment first and do not spend analysis budget on TUIC, QUIC,
  smoltcp, MTU, or concurrency branches.

## 2026-07-03 — c90471c exposed relay-writer small-write amplification

- Log bundle: `/tmp/mvpn_knife14v_usclient_suite_20260703_103350.tar.gz`
- Tested commit: `c90471c`
- Symptom: reverse TCP was healthy at roughly 153-183 Mbit/s receiver, but
  forward TCP stayed near 1-3 Mbit/s receiver with long zero-bps gaps.
- Important discriminator: `remote_write_timeout` was absent; forward P1 close
  diagnostics showed roughly 10.5 MB upstream across 8529 writer calls, so the
  path was dominated by many 1160-byte class `write_all` operations.
- Rejected assumption: once established-uplink reads are batched in the main
  loop, forward throughput is no longer limited by per-payload scheduling.
- Correct behavior: coalesce already queued relay `Data` commands into bounded
  writer batches, preserve `Finish` order, and log local write wait pressure on
  relay close.
- Future debugging rule: when forward throughput shows burst-then-silence
  windows without write timeouts, compare `uplink_bytes` with `uplink_writes`
  before changing TUIC pool, MTU, congestion control, or VPS service settings.

## 2026-07-02 — a57873a exposed premature local Finish on reverse traffic

- Log bundle: `/tmp/mvpn_knife14c_usclient_suite_20260702_223847.tar.gz`
- Tested commit: `a57873a`
- Symptom: forward P1 recovered, but reverse P1 stayed at Kbit/s scale and
  reverse P2/P4/P8 timed out or transferred zero.
- Important discriminator: the only `dead_slot_reap pending>0` was
  `tcp_state=Closed active=false can_send=false`, so the knife14u pending-reap
  branch was not the reverse bottleneck.
- Rejected assumption: once smoltcp reports local `CloseWait`, immediately
  sending upstream `Finish` is harmless for reverse traffic.
- Correct behavior: defer `Finish` while remote-to-local data is making progress;
  send it only after a short remote-quiet window, then let the existing
  half-closed idle timeout bound cleanup.
- Future debugging rule: when reverse traffic collapses after a tiny burst, look
  for early local FIN/half-close propagation before changing QUIC flow-control or
  buffer sizes.

## 2026-07-02 — Knife14t showed generic pending grace can preserve dead local tails

- Log bundle: `/tmp/mvpn_knife14t_usclient_suite_20260702_215901.tar.gz`
- Tested commit: `51072c8`
- Symptom: VPS preflight was healthy, but tunnel throughput regressed badly and
  reverse tests showed Kbit/s-scale windows/timeouts. `dead_slot_reap pending>0`
  remained, with smaller pending values than knife14s.
- Rejected assumption: if inactive `downlink_pending` had recent progress, it is
  always worth preserving for the full grace window.
- Correct behavior: split inactive pending by local send capability. If
  `TcpSocket::can_send()` is false while the socket is inactive, the pending
  bytes are no longer deliverable and should be reaped immediately; if it is
  true, keep the existing bounded progress-sensitive grace.
- Future debugging rule: pending-byte lifecycle logs must include enough local
  socket state (`tcp_state`, `active`, `can_send`) to tell whether a close
  dropped useful tail bytes or correctly discarded undeliverable bytes.

## 2026-07-02 — Full-repo cargo fmt creates unrelated churn in this workspace

- Symptom: running `cargo fmt` during knife14u rewrote many unrelated source
  files and inflated the diff far beyond the data-plane change.
- Correct behavior: avoid full-repo formatting unless the stage explicitly owns
  that cleanup. For scoped fixes in this repository, keep existing local style
  and rely on tests, clippy, and `git diff --check` unless formatting is needed
  for the touched hunk.
- Future debugging rule: inspect `git diff --stat` after any formatting command
  before continuing; revert accidental churn immediately.

## 2026-07-02 — Knife14s exposed a non-deferred pending downlink reap hole

- Log bundle: `/tmp/mvpn_knife14s_usclient_suite_20260702_180337.tar.gz`
- Tested commit reported by the suite: `6ff49cf`
- Symptom: client diagnostics still showed `tcp-handle-close ... reason=dead_slot_reap ...
  pending>0`, including `state=Relaying pending=1348494`,
  `state=Relaying pending=1307798`, `state=Relaying pending=1258063`, and
  smaller `state=Closing pending>0` cases.
- Important discriminator: those lines had `send_slice_errors=0` and
  `tun_flush_tx_failures=0`; the backlog was not being cleared by a local send
  failure path.
- Rejected assumption: only `pending_relay_close` needs progress-sensitive grace.
- Correct behavior: non-empty `downlink_pending` should have generic progress
  metadata and should be reapable only after no observation/accepted-byte
  progress for the grace window when the socket is inactive.
- Future debugging rule: whenever logs show `pending>0` at close, split the
  branch by lifecycle state (`Relaying`, `Closing`, `CloseWait`, `Closed`) before
  assuming one terminal-event guard covers them all.

## 2026-07-02 — Knife14r exposed one-payload-per-tick uplink throttling

- Log bundle: `/tmp/mvpn_knife14r_usclient_suite_20260702_171341.tar.gz`
- Symptom: forward P1/P4/P8 stayed around low Mbit/s with long zero-bps gaps,
  and P2 timed out. `remote_write_timeout` was absent, so this was not the old
  QUIC write-deadline failure.
- Root cause in code: established uplink drained at most one smoltcp payload per
  dirty pass. Without continuous inbound wakeups, the 5ms timer became the
  effective throughput limiter.
- Correct behavior: drain a bounded batch per dirty pass, but reserve relay mpsc
  capacity before reading each payload so channel fullness still applies TCP
  backpressure.
- Future debugging rule: when measured throughput is suspiciously close to
  `MSS * timer frequency`, inspect event-loop batching before touching QUIC,
  congestion control, or VPS tuning.

## 2026-07-02 — Knife14r report could not prove the exact binary commit

- Log bundle: `/tmp/mvpn_knife14r_usclient_suite_20260702_171341.tar.gz`
- Symptom: the report showed `SUITE_TAG=knife14r`, but did not include
  `git rev-parse` output or binary checksum. The script also only built the
  release binary if it was missing.
- Correct behavior: test suites should record git commit, worktree status,
  binary path, and checksum; `BUILD_RELEASE=1` should rebuild before running.
- Future debugging rule: never analyze a VPS performance run as definitive until
  the report proves the binary came from the intended commit.

## 2026-07-02 — Knife14q showed fixed deferred-close grace still drops useful tail bytes

- Log bundle: `/tmp/mvpn_knife14q_usclient_suite_20260702_160853.tar.gz`
- Symptom: uplink `remote_write_timeout` was gone, but client diagnostics still
  showed `tcp-handle-close ... reason=dead_slot_reap state=Closing pending>0`
  with no corresponding send-slice or TUN flush failure.
- Rejected assumption: "relay close plus pending downlink only needs a fixed
  grace window from close time."
- Correct behavior: if pending downlink is still decreasing, refresh the reap
  deadline; reap only after no drain progress for the grace window.
- Future debugging rule: for pending-buffer lifecycle bugs, track progress
  counters and last-progress time, not just absolute age.

## 2026-07-02 — Knife14p VPS run exposed a wrong per-write timeout assumption

- Log bundle: `/tmp/mvpn_knife14p_usclient_suite_20260702_143215.tar.gz`
- Tested commit: `480c3e8`
- Symptom: throughput collapsed/timeouts while client logs contained
  `remote_write_timeout attempted_bytes=1160 timeout=5s`.
- Rejected assumption: a TUIC/QUIC stream write pending for 5s means the relay is
  broken.
- Correct behavior: pending writes are normal backpressure; keep the writer
  awaitable and let bounded relay-level idle/cleanup paths terminate true stalls.
- Future debugging rule: when a stage adds backpressure, verify which direction
  the evidence points to. `global_rx_pressure_events=0` meant knife14p's
  downlink mechanism was not firing; the next fix belonged in the uplink writer.

## 2026-07-04 — Knife14au repeated the full-repo cargo fmt churn risk

- Symptom: running `cargo fmt` at the repository root reformatted many unrelated
  Rust files and expanded the diff beyond the pending-at-close observability
  slice.
- Fix: reverted the formatter-only churn before committing, then reapplied only
  the `src/client_tun.rs` behavior-neutral logging/test changes.
- Future debugging rule: for scoped Knife14 fixes, do not run full-repo
  formatting unless the stage explicitly owns that cleanup. Prefer existing
  local style plus `cargo test`, clippy, script self-tests, shell syntax checks,
  and `git diff --check`.

## 2026-07-04 — Knife14au per-probe summary missed a post-iperf close-tail

- Log bundle: `/tmp/mini_vpn/mvpn_knife14au_pending_close_taxonomy_usclient_suite_20260704_193706.tar.gz`
- Symptom: full reverse raw log contained `tcp-handle-close pending>0` with
  `close_pending_class=terminal_closed_no_send`, but that probe's attribution
  summary reported `pending_at_close=0`.
- Root cause: `scripts/knife14b-lowrtt-probe.sh` sampled TUN counters and wrote
  metrics/attribution immediately after `iperf3` exited. The close-tail log
  arrived shortly afterward and only appeared in the final post-run metric tail.
- Correct behavior: wait a short, bounded post-iperf settle window before
  per-probe metrics and attribution summaries. Sample final TUN counters after
  that window so tail flush/drop effects stay in the same probe.
- Future debugging rule: if a raw log and a per-probe summary disagree, inspect
  the report timing before changing Rust lifecycle, pacing, TUIC pool, iperf3,
  or sing-box behavior.

## 2026-07-04 — Knife14ba repeated scoped-change cargo fmt churn

- Symptom: running root `cargo fmt` during the scoped diagnostics stage
  reformatted many unrelated Rust files, including REALITY and failover modules,
  and expanded the diff far beyond the intended `client_tun`, `tuic`, and parser
  changes.
- Fix: restored the unrelated formatter-only churn and reapplied the
  `client_tun.rs` relay timing patch as a minimal diff.
- Future debugging rule: for Knife14 scoped changes, do not run root
  `cargo fmt`. If formatting is necessary, format only the touched hunks or
  accept existing local style, then use `git diff --stat` and `git diff --check`
  to catch whitespace issues.

## 2026-07-04 — Knife14ba stream-gap attribution counted idle control streams

- Bundle: `/tmp/mini_vpn/mvpn_knife14ba_stream_timing_usclient_suite_20260704_231537.tar.gz`
- Symptom: clean reverse-first attribution included `tuic_stream_read_gap` and
  `relay_remote_read_gap`, but the >30s gap came from a tiny iperf control
  stream (`rx_bytes=343`). The data stream had `rx_bytes=110790062` and
  `max_read_gap_ms=3763`.
- Root cause: the parser grouped all TUIC TCP streams and relay remote-read
  streams together, so an idle control stream could label the probe as a data
  read-gap failure.
- Correct behavior: split or filter small control streams before assigning
  read-gap attribution. Data-stream read-gap labels should require enough
  received bytes to represent the throughput stream.
- Future debugging rule: if stream timing diagnostics show a large gap, always
  pair the gap with stream byte volume before treating it as the performance
  root.

## 2026-07-04 — Knife14bb fixed control-stream read-gap over-attribution

- Fix: `scripts/knife14b-lowrtt-probe.sh` now keeps all-stream timing maxima in
  the summary but uses data-bearing streams/handles (`>=65536` received bytes)
  for reverse TCP read-gap attribution.
- Regression test: the parser self-test now includes the Knife14ba shape where
  a `343B` control stream has `30147ms` gap and a `110MB` data stream has
  `3763ms` gap. The expected attribution excludes `tuic_stream_read_gap` and
  `relay_remote_read_gap`.
- Future debugging rule: when adding parser labels, include a negative fixture
  for the nearest known false-positive shape before trusting new attribution in
  VPS reports.

## 2026-07-04 — Knife14bc left a parser helper as production dead code

- Symptom: after `from_env` switched to the tx-buffer-aware backpressure parser,
  full `cargo test` emitted a `dead_code` warning for the old fixed-default
  `parse_downlink_backpressure_config` helper. `cargo clippy -D warnings` would
  have failed if this had been left in place.
- Fix: mark the fixed-default helper `#[cfg(test)]`; production now uses the
  tx-buffer-aware parser while legacy default behavior remains covered by
  tests.
- Future debugging rule: after replacing a production config path, check whether
  any helper became test-only and gate it before running clippy.

## 2026-07-04 — Knife14bc VPS run would have been masked by suite env defaults

- Symptom: before running VPS acceptance, inspection showed
  `scripts/knife14b-usclient-tunnel-suite.sh` exported fixed
  `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288` and
  `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`, which would override the
  Knife14bc adaptive binary defaults.
- Fix: Knife14bd changed the suite defaults to empty `<auto>` values while
  preserving explicit operator overrides.
- Future debugging rule: after a config-default patch, inspect the acceptance
  script's exported env vars before running VPS. A script-level default can
  silently invalidate the code path under test.

## 2026-07-04 — .27 VPS does not have ripgrep installed

- Symptom: `ssh ... 'rg ...'` on `.27` failed with `bash: line 1: rg: command
  not found`.
- Correct behavior: use portable `grep`/`find` commands on VPS hosts unless
  `rg` availability has been checked.

## 2026-07-04 — sudo VPS suites need a real tool TTY from the start

- Symptom: the first Knife14bd suite attempt used `ssh -tt`, but the local
  command was not started with a writable tool TTY. `sudo -v` prompted for a
  password, stdin closed, and the resulting bundle was incomplete.
- Correct behavior: when a `.27` suite may need sudo, start the command with a
  true TTY session at the tool level from the beginning, then enter the password
  only at the sudo prompt. Do not treat any non-TTY sudo-failed bundle as
  acceptance evidence.

## 2026-07-04 — 1MiB adaptive downlink backpressure did not fix Knife14 throughput

- Symptom: Knife14bd clean reverse-first with auto watermarks
  `high=1048576B low=262144B` still achieved only `27.8/26.2 Mbit/s` and ended
  with `terminal_pending_reap=1058416B`.
- Rejected interpretation: the remaining clean root is not the legacy 512KiB
  suite override alone; that was fixed and the binary startup confirmed the
  1MiB watermarks.
- Correct behavior: analyze the failed run before further code changes. The
  next patch/run must distinguish tx-buffer/receive-window capacity from local
  TCP/TUN drain cadence; do not keep changing close-drain, pool, sing-box,
  iperf3, TUN queue length, or egress pacing without new evidence.

## 2026-07-05 — Knife14be repeated the missing tool-level TTY mistake

- Symptom: the first Knife14be suite attempt used `ssh -tt`, but the local tool
  command omitted `tty=true`. The run blocked at `sudo -v`, stdin was closed,
  and the partial bundle
  `/tmp/conn/mvpn_knife14be_tx4m_auto_usclient_suite_20260705_065544.tar.gz`
  is invalid.
- Correct behavior: any `.27` suite that may need sudo must start with both
  remote `ssh -tt` and tool-level `tty=true`. If this is missed, kill the remote
  suite process and discard the partial bundle.

## 2026-07-05 — 4MiB receive-window A/B introduced clean TUN egress drops

- Symptom: Knife14be clean reverse-first with `tx=4194304B` and auto
  backpressure `high=4194304B low=1048576B` achieved only `20.3/18.9 Mbit/s`
  and reported `tun_tx_dropped_delta=27530`.
- Rejected interpretation: larger local TCP tx buffers are not the Knife14 fix.
  The 4MiB run worsened clean throughput and moved the failure to TUN/qdisc
  loss while QUIC remained clean.
- Correct behavior: stop increasing tx buffers. The next repair must first
  analyze and plan local TCP/TUN drain cadence or TUN-drop-aware feedback, then
  add deterministic accounting/parser tests before another VPS run.

## 2026-07-05 — Knife14bf repeated root cargo fmt churn

- Symptom: running root `cargo fmt` during the Knife14bf scoped patch
  reformatted many unrelated Rust files and expanded the diff well beyond the
  intended `client_tun`/parser/docs task.
- Fix: restored the unrelated files and then restored/reapplied
  `src/client_tun.rs` as a minimal hand patch. No root `cargo fmt` output was
  kept in the final diff.
- Correct behavior: do not run root `cargo fmt` in Knife14 scoped stages. If a
  patch needs formatting, keep the local style or format only the precise
  touched hunk, then rely on `cargo test`, script self-tests, and
  `git diff --check`.

## 2026-07-05 — Knife14bf initial VPS run failed at TUIC auth finish

- Symptom: the first `.27` Knife14bf suite on `e661613` failed before data
  plane acceptance with `tuic auth finish: sending stopped by peer: error 0`.
- Follow-up: `.33` sing-box was active and client/server credential hashes
  matched without exposing secrets. Restarting sing-box cleared the startup
  failure; the rerun connected and produced a valid bundle.
- Correct behavior: when TUIC startup fails with `auth finish: sending stopped
  by peer` while service health and credential hashes match, restart sing-box
  once and rerun before invalidating the client code. Do not record the failed
  startup bundle as throughput evidence.

## 2026-07-05 — .27 lacks GitHub deploy-key fetch access

- Symptom: syncing `.27` directly from GitHub over SSH failed because the VPS
  key was not accepted for the repository.
- Workaround: created a local git bundle for the Mac commit range, copied it to
  `.27`, then used `git fetch /tmp/... HEAD` plus `git merge --ff-only
  FETCH_HEAD`.
- Correct behavior: use the bundle sync path for `.27` until repository SSH
  access is deliberately configured on that host. Do not keep retrying
  interactive GitHub authentication from the VPS.

## 2026-07-05 — Knife14bg TUN RX drain default can starve reverse downlink

- Symptom: Knife14bh A/B on `7e33d44` showed
  `MINI_VPN_TUN_RX_DRAIN_BUDGET=8` collapsed clean reverse-first P1 to
  `0.245/0.035 Mbit/s`; data stream first useful RX was delayed about
  `24.5s`, and `tun_rx_drain` consumed `53` TCP packets in the window.
- Rejected interpretation: the bg drain path is not merely a harmless fairness
  helper. It changes the clean reverse-first failure mode and can make the data
  stream appear TUIC-starved.
- Correct behavior: flip the product/suite default drain budget to `0` before
  further lifecycle or receive-window work. Keep the drain path only as an
  explicit env-gated A/B tool until a safer scheduling design exists.

## 2026-07-05 — Root cargo fmt --check is not a Knife14 scoped gate

- Symptom: `cargo fmt --check` reported thousands of repo-wide formatting diffs
  including unrelated files, matching the earlier Knife14bf fmt-churn hazard.
- Correct behavior: do not run or apply root `cargo fmt` in Knife14 scoped
  stages. Use `git diff --check`, focused tests, clippy, and hand-format the
  touched hunks only.

## 2026-07-05 — Knife14bi default VPS run failed before throughput at TUIC startup

- Symptom: the `.27` Knife14bi default suite on `76af8dc` failed before the
  reverse-first probe with
  `tuic auth finish: sending stopped by peer: error 0`.
- Evidence: `.33` sing-box was active, had `NRestarts=0`, passed config check,
  listened on UDP `8443`, used a valid server certificate, and a no-secret exact
  comparison reported matching UUID/password/SNI/ALPN between `.27` env and
  `.33` config.
- Correct behavior: do not treat this bundle as throughput evidence and do not
  change mini_vpn lifecycle/backpressure code for this failure. Restart
  `.33` sing-box once and rerun the identical scoped suite; if it fails again,
  stop for TUIC startup compatibility analysis.

## 2026-07-05 — Nested SSH config comparison must not expose TUIC secrets

- Symptom: a first attempt to summarize `.33` config with remote Python failed
  due escaped newline syntax, and a second nested SSH `jq` comparison failed
  because the remote shell interpreted the unquoted jq filter's pipes.
- Safety catch: an attempted grep that would have printed `uuid` and
  `password` lines was rejected. That rejection was correct.
- Correct behavior: when exact credential comparison is needed, feed a short
  script to remote `python3 -` over SSH stdin and print only boolean
  match/mismatch fields. Never print TUIC credentials or hashes in logs,
  reports, learnings, or summaries.

## 2026-07-05 — Knife14bi default drain-off did not reproduce bh drain0 throughput

- Symptom: after restarting `.33`, the Knife14bi default rerun started
  successfully with `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`, but reverse-first P1
  collapsed to `0.280/0.017 Mbit/s`.
- Rejected interpretation: this is not the old bg TUN RX drain regression and
  not a close-drain/terminal-pending loss point. The run had
  `tun_rx_drain attempts=0`, no downlink backpressure, no TUN drops, no
  terminal pending reap, no send-slice errors, and no QUIC loss/blocking.
- Correct behavior: stop treating drain-off as sufficient proof. Before the
  next VPS run, add or inspect instrumentation that explains why the TUIC data
  stream receives only about `86KB` despite healthy direct baselines and clean
  local egress counters.

## 2026-07-05 — Knife14bj clippy caught a widening formatter argument list

- Symptom: `cargo clippy --lib -- -D warnings` failed after adding TUIC stream
  poll-cadence fields because `format_tuic_tcp_stream_close_line` grew to nine
  positional arguments and triggered `clippy::too_many_arguments`.
- Fix: pass the existing `TuicTcpStreamCloseSnapshot` into the close-line
  formatter instead of extending the positional parameter list.
- Correct behavior: when adding multiple diagnostic counters to an existing
  formatter, prefer a snapshot/struct parameter over more positional arguments,
  then run clippy before committing.

## 2026-07-05 — Sandbox blocks UDP bind for QUIC endpoint tests

- Symptom: sandboxed `cargo test --lib` failed only
  `quic::tests::client_endpoint_binds` and
  `quic::tests::client_endpoint_binds_with_each_cc`; a direct sandboxed
  `python3` UDP bind to `0.0.0.0:0` also failed with `Operation not permitted`.
- Evidence: rerunning `cargo test --lib` outside the sandbox passed all
  `282` lib tests.
- Correct behavior: if QUIC endpoint bind tests fail under the managed sandbox,
  verify with a sandbox-external `cargo test` before changing QUIC endpoint or
  transport code. Treat the sandbox failure as test-environment evidence, not
  a product regression.

## 2026-07-05 — .27 non-login SSH shell does not expose cargo

- Symptom: `ssh ... 'cd /home/ubuntu/mini_vpn && cargo build --release'`
  failed on `.27` with `cargo: command not found`.
- Fix: rerun through a login shell:
  `bash -lc "cd /home/ubuntu/mini_vpn && cargo build --release"`.
- Correct behavior: for `.27` remote builds, use `bash -lc` or explicitly
  source the Rust environment before invoking cargo. Do not treat a non-login
  shell `cargo` miss as a Rust build failure.

## 2026-07-05 — Knife14bj poll diagnostics did not fix reverse starvation

- Symptom: the `4dc79af` scoped reverse-first VPS suite measured only
  `0.210/0.019 Mbit/s` even though `.27 -> .77` and `.33 -> .77` baselines
  were healthy and `.33` reported no sampled `fail auth`.
- Rejected interpretation: this bundle is not evidence for stale pool, TUN RX
  drain, local TUN egress drops, global_rx pressure, downlink backpressure,
  egress pacing, or clean-window QUIC loss/congestion. The data stream was
  periodically polled (`data_poll_gap_max_ms=5000`) while its pending/read gaps
  reached about `20.5s`.
- Correct behavior: before any behavior code patch, add or collect
  server-side/send-side diagnostics that prove whether `.77` sent little data,
  sing-box stopped forwarding, or mini_vpn/quinn did not receive stream-ready
  data. Do not keep patching local lifecycle paths without that evidence.

## 2026-07-05 — Knife14bk acceptance still low, but failure shape changed

- Symptom: the `804eec1` server-evidence suite completed but reverse-first P1
  was still only `18.8/17.0 Mbit/s`.
- Evidence: `.77` journal showed its reverse sender also finished at
  `67.1 MBytes / 18.8 Mbit/s`; `.33` showed TUIC inbound/direct outbound opens
  and no current `fail auth`; mini_vpn showed `tun_tx_dropped_delta=6070`,
  `downlink_backpressure pause_edges=10`, and `active_no_send` close pending.
- Rejected interpretation: this run is not proof that `.77` sent hundreds of
  Mbit/s and mini_vpn silently lost the bytes, and it is not a terminal
  pending-reap loss point.
- Correct behavior: before changing data-plane behavior, propose a small local
  TUN egress/backpressure plan that targets smoltcp send-queue saturation and
  TUN qdisc drops. Also tighten `.33` server-evidence log bounding because the
  current tail-based capture includes unrelated older VLESS noise.

## 2026-07-05 — Root cargo fmt creates unrelated repository-wide churn

- Symptom: running `cargo fmt` at the repository root during Knife14bl rewrote
  many unrelated Rust files, producing a large diff outside the scoped
  downlink/TUN egress task.
- Fix: reverse only the agent-created formatting churn and reapply the scoped
  Knife14bl patch manually.
- Correct behavior: on this branch, do not use root `cargo fmt` as a default
  gate for narrow data-plane patches. Prefer `git diff --check`, focused tests,
  and manual/targeted formatting unless the intended task is repository
  formatting.

## 2026-07-05 — ssh -tt alone is not enough for sudo prompt input through exec

- Symptom: the first Knife14bl suite launch used remote `ssh -tt`, but the local
  exec session did not set `tty=true`; stdin was closed when sudo prompted for
  the `.27` password.
- Fix: terminate that hung SSH process and rerun the identical command with
  `tty=true`, then enter the password only at the sudo prompt.
- Correct behavior: whenever a `.27` suite may need `sudo -v`, set both remote
  `ssh -tt` and local exec `tty=true` from the beginning. Do not rely on
  `ssh -tt` alone.

## 2026-07-05 — Knife14bm failed in a non-comparable no-data-stream shape

- Symptom: the `3d06bea` recent-pressure VPS run measured only
  `1.19/0.00 Mbit/s`; `.77` sender stopped after `4.25 MiB`, mini_vpn data
  stream first RX arrived after about `37.6s`, and the clean window had no TUN
  drops, downlink backpressure, QUIC loss, or terminal pending.
- Rejected interpretation: this is not evidence that the recent-pressure latch
  made TUN egress worse, and it is not a valid replay of Knife14bl's
  `130 Mbit/s` tail-collapse with sampled TUN drops.
- Correct behavior: after a repair run changes failure shape this sharply,
  stop before new behavior-code edits. Record the result, compare same-window
  behavior or tighten attribution, then proceed only after confirming the next
  plan.

## 2026-07-05 — raw SHA bundle creation can produce an empty bundle

- Symptom: `git bundle create /tmp/... 09bb67c` and the same command for
  `3d06bea` failed with `fatal: Refusing to create empty bundle`.
- Cause: a raw commit SHA is not a bundle ref by itself for this usage.
- Correct behavior: create a bundle from a real ref such as `HEAD` when the
  target commits are reachable, then fetch that bundle on `.27` and switch to
  the desired commit SHA locally.

## 2026-07-05 — .27 suite commands must source .env explicitly

- Symptom: the first Knife14bn `09bb67c` suite failed before throughput because
  `MINI_VPN_TUIC_*` env vars were missing.
- Cause: the remote command did not source `/home/ubuntu/mini_vpn/.env`; an SSH
  login shell did not export those variables automatically.
- Correct behavior: when running the suite manually from `.27`, start from the
  repo root and run `set -a; . ./.env; set +a` before invoking the suite. Treat
  missing TUIC env bundles as invalid pre-throughput artifacts.

## 2026-07-05 — cargo fmt --check is still a noisy gate on this branch

- Symptom: Knife14bo `cargo fmt --check` failed with repository-wide formatting
  diffs in unrelated Rust files, including files outside the scoped
  close-drain change.
- Cause: the current branch still contains historical non-rustfmt formatting,
  so even check-only formatting is not a useful narrow-stage gate.
- Correct behavior: keep using `git diff --check`, focused tests, parser
  self-tests, and sandbox-external `cargo test --lib` for this Knife14 branch.
  Do not run or apply root formatting unless repository formatting is the
  explicit task.

## 2026-07-05 — SERVER_EVIDENCE_CHECK needs SSH host envs

- Symptom: the Knife14bo suite was launched with `SERVER_EVIDENCE_CHECK=1`, but
  its server-evidence artifact skipped both `.33` sing-box and `.77` iperf3
  collection because `EXIT_SSH_HOST` and `TARGET_SSH_HOST` were unset.
- Fix: manually collected `.33` and `.77` evidence for the probe time window
  after the run.
- Correct behavior: when requesting server evidence on the known VPS topology,
  also pass `EXIT_SSH_HOST=ubuntu@43.153.32.33`,
  `TARGET_SSH_HOST=ubuntu@43.130.32.77`, and
  `EXIT_SSH_KEY=/home/ubuntu/.ssh/vpn` / `TARGET_SSH_KEY=/home/ubuntu/.ssh/vpn`
  from `.27`, or teach the suite to default these values for the Knife14
  acceptance hosts.

## 2026-07-05 — Knife14bp evidence run changed to no-data shape

- Symptom: the `d5d8542` evidence-defaults suite had healthy direct baselines
  but reverse-first P1 over mini_vpn measured only `280 Kbit/s` sender and
  `2.64 Kbit/s` receiver.
- Evidence: server evidence was complete. `.33` showed current TUIC
  inbound/direct outbound with no `fail auth`; `.77` iperf3 journal showed the
  target sender itself at `1.00 MBytes / 280 Kbit/s`. mini_vpn had no TUN
  drops, no downlink backpressure, no QUIC loss/congestion, no pending-at-close,
  and no terminal pending reap. Terminal-late payload was visible and bounded:
  `28544B` across `2` events.
- Correct behavior: do not treat this run as a clean tx-queue-only
  receive-window branch and do not make a behavior patch from it alone. First
  repeat in the same evidence mode or add evidence-only stream/window
  instrumentation to distinguish VPS run variance from a deterministic
  stream-readiness or local TCP ACK/window issue.

## 2026-07-06 — Knife14bq repeat changed from no-data to tail-collapse pressure

- Symptom: the same `d5d8542` reverse-first P1 repeat with server evidence and
  `.33 <-> .77` path checks averaged `150/149 Mbit/s`, but the final six
  seconds collapsed to about `15.7-16.8 Mbit/s`.
- Evidence: `.27 <-> .77` and `.33 <-> .77` baselines were healthy, `.33`
  showed current TUIC inbound/direct outbound to `.77:5201` and no current TUIC
  `fail auth`, `.77` sender matched `537 MBytes / 150 Mbit/s`, QUIC
  loss/congestion stayed `0/0`, while mini_vpn recorded
  `tun_tx_dropped_delta=14804`, `downlink_backpressure=693/693`, and
  `terminal_late_remote_payload=1474528B`.
- Correct behavior: do not continue the no-data branch and do not treat the
  average `149 Mbit/s` as final acceptance. The next patch must be
  evidence/TDD-first around local TUN egress pressure, downlink pause/resume
  timing, and close-tail terminal-late correlation before changing behavior.

## 2026-07-06 — throughput-shape tests must match probe direction

- Symptom: during Knife14br TDD, a new `no_data` assertion was first attached to
  an existing late-remote fixture that was not a reverse TCP iperf sample, so the
  parser correctly returned `throughput_shape: shape=unknown`.
- Cause: the new shape classifier is intentionally scoped to reverse TCP probes;
  forward and UDP samples keep attribution labels but do not receive reverse
  throughput-shape semantics.
- Correct behavior: put `no_data`, `tail_collapse`, and `stable_high`
  throughput-shape assertions on reverse TCP fixtures only. Use forward fixtures
  for relay/late-remote behavior, not reverse-throughput acceptance shape.

## 2026-07-06 — Stale downlink backpressure env can bypass auto defaults

- Symptom: Knife14bq was expected to exercise tx-buffer-scaled downlink
  backpressure, but the report and startup log showed
  `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576` with high `524288` and low `131072`.
  The run then produced `downlink_backpressure=693/693`, TUN drops, and tail
  collapse.
- Cause: inherited `.env` or shell variables explicitly set the old
  `524288/131072` pair, so `client_tun.rs` correctly honored explicit config
  instead of applying its `<auto>` scaling.
- Correct behavior: Knife14 acceptance suites must normalize only this legacy
  pair back to `<auto>` under a larger tx buffer unless
  `KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=1` is set. Future result
  analysis must check the actual startup high/low line before blaming
  receive-window code.

## 2026-07-06 — TUIC auth finish can fail once despite matching config

- Symptom: the first Knife14bs VPS run on `dd9c6ad` failed during mini_vpn
  startup with `tuic auth finish: sending stopped by peer: error 0`.
- Evidence: `.33` sing-box was active with UDP `:8443` listening, `.27/.33`
  time skew was `0s`, sing-box config check passed, and a manual no-secret
  comparison showed `uuid_match=1`, `password_match=1`, `sni_match=1`, and
  `alpn_match=1`. A no-build retry immediately afterward connected and reached
  the reverse-first probe.
- Correct behavior: if this exact startup failure appears once while no-secret
  config/time/service checks pass, treat it as a startup transient and retry
  once before changing code or restarting sing-box. If repeated, then stop and
  inspect `.33` service/runtime logs before more acceptance runs.

## 2026-07-06 — No-secret TUIC compare error is inconclusive by itself

- Symptom: the first Knife14bt VPS run on `b5752c4` failed with the same
  `tuic auth finish: sending stopped by peer: error 0`, and the generated
  no-secret compare script printed `exit_config_compare_error=1` with
  `CalledProcessError`.
- Evidence: service diagnostics still showed `.27/.33` time skew `0s`, `.33`
  sing-box active, UDP `:8443` listening, and sing-box config check passing. A
  manual rerun of the no-secret compare with explicit SSH env immediately
  returned `uuid_match=1`, `password_match=1`, `sni_match=1`, and
  `alpn_match=1`; the no-build retry then connected.
- Correct behavior: treat `exit_config_compare_error=1` as an inconclusive
  diagnostic failure, not as an auth mismatch. Rerun the no-secret compare with
  explicit `EXIT_SSH_*` env and only blame sing-box/config if the match booleans
  fail or the startup failure repeats after one retry.

## 2026-07-06 — Tx-buffer-scaled high watermark can regress reverse throughput

- Symptom: with suite normalization forcing binary auto defaults,
  `high=1048576B low=262144B`, reverse-first P1 fell to `18.8 Mbit/s` receiver
  with periodic zero-throughput intervals.
- Evidence: direct and exit-target baselines stayed healthy; QUIC
  loss/congestion/blocking stayed zero; global_rx pressure stayed zero; TUN
  drops reduced to `586`, but smoltcp `send_queue` hit `1048576`, stream read
  gaps reached `3782ms`, and target sender cwnd collapsed in the same stop/go
  pattern.
- Correct behavior: do not increase receive-window/high watermark as a default
  throughput fix without TUN/qdisc-capacity evidence. The next patch must make
  local egress pressure durable across poll/flush or explicitly tune high/low
  against measured TUN egress capacity.

## 2026-07-06 - Knife14bu pressure hold failed VPS acceptance

- Stage: Knife14bu durable egress pressure acceptance for commit `fd3f2ed`.
- Failed bundle:
  `/tmp/mini_vpn/knife14bu_durable_20260706/mvpn_knife14bu_durable_usclient_suite_20260706_190244.tar.gz`
- Symptom: reverse-first P1 reached only `17.3/15.3 Mbit/s` with the same
  low-average stop/go profile.
- Important discriminator: the pressure hold eliminated TUN tx drops
  (`tun_tx_dropped_delta=0`) and terminal late payload
  (`terminal_late_remote_payload=0`), but throughput worsened, pause/resume
  churn increased to `63/62`, and `tun_flush_deferred` rose to `62`.
- Close-boundary signal: the data handle closed with `tcp_state=CloseWait`,
  `may_recv=false`, `can_send=true`, `may_send=true`, `send_queue=524288`,
  `pending=0`, and `terminal_pending_reap_bytes=0`.
- Rejected next moves: do not keep extending the pressure hold, changing
  scripts, tuning stale pool, iperf3, sing-box, TUN qlen, or QUIC from this
  evidence.
- Correct behavior: stop before the next behavior edit. First clean the release
  warning, add raw/effective pressure and close-time egress observability, then
  TDD a bounded close/egress-drain rule for send-capable `CloseWait` sockets.

## 2026-07-06 - Release build warnings must be cleaned before VPS evidence

- Symptom: the `.27` release build for Knife14bu succeeded but printed
  `warning: method observe_pressure is never used`.
- Cause: the compatibility wrapper is used only by tests after production code
  moved to the timestamped `observe_pressure_at` path.
- Correct behavior: future VPS-bound commits should leave release builds
  warning-clean. Remove unused wrappers or gate test-only helpers with
  `#[cfg(test)]` before running acceptance, so warning noise does not blur
  operational evidence.

## 2026-07-06 - Repo-wide rustfmt check is not a Knife14 gate

- Symptom: `cargo fmt --check` failed before the Knife14bv-b commit by printing
  a repo-wide formatting diff across existing Rust files, including files
  outside the close-egress lifecycle patch.
- Cause: the repository has pre-existing global rustfmt drift; applying
  `cargo fmt` would create a large unrelated formatting change and obscure the
  small lifecycle patch.
- Correct behavior: do not use repo-wide `cargo fmt --check` as a blocking
  Knife14 gate until formatting is normalized in a dedicated task. For these
  lifecycle patches, keep edits narrow and use `cargo test/build`,
  script self-tests/syntax checks, and `git diff --check`.

## 2026-07-06 - Knife14bv no-data run was not a close-egress candidate

- Symptom: Knife14bv acceptance for `8f68a89` regressed reverse-first P1 to
  `0.315/0.113 Mbit/s` with `throughput_shape=no_data`.
- Evidence: `downlink_backpressure=0/0`, TUN drops `0`, send-slice errors `0`,
  QUIC client-side loss/congestion/blocking `0`, and no
  `tcp-deferred-close-egress` line. The only close-egress accounting was a
  terminal closed/no-send `14824B`, while TUIC stream 4 showed
  `max_read_gap_ms=20567` and only `723424B` total rx.
- Rejected next move: do not extend the close-egress drain grace, tune
  close/reap thresholds, or chase sing-box auth from this evidence. `.33`
  current-window TUIC auth was clean and the bottleneck appears before the
  close boundary.
- Correct behavior: stop before behavior changes. First add diagnostics/TDD
  that distinguish TUIC server-to-client stream starvation from mini_vpn local
  TCP ACK/receive-window collapse, then run one scoped acceptance before
  choosing a behavior patch.

## 2026-07-06 - Soft tx_queue pressure must still feed TUN drop attribution

- Symptom: during Knife14bx local gates, after splitting tx_queue-only
  backpressure to use a hard cap, the focused
  `tun_egress_feedback_pauses_on_drop_delta_with_recent_high_pressure` test
  failed.
- Cause: the first implementation reused the new hard-cap predicate for both
  the 25ms remote-read pressure hold and the recent-pressure sample used by
  TUN drop feedback. That made a real TUN `tx_dropped` sample after soft-high
  pressure look pressure-free.
- Correct behavior: keep these two roles separate. Record recent pressure for
  drop attribution whenever raw pressure reaches the conservative soft high,
  but install the 25ms egress hold only when app pending reaches high or
  tx_queue-only pressure reaches its derived hard cap.

## 2026-07-06 - Knife14bx tx_queue headroom still left flush deferral at soft high

- Symptom: Knife14bx VPS acceptance improved reverse-first P1 to
  `27.4/26.4 Mbit/s`, but it still failed with `throughput_shape=low_average`.
- Evidence: app pending stayed `0`, TUN drops stayed `0`, QUIC
  loss/congestion/blocking deltas stayed `0`, send-slice zero/errors stayed
  `0`, and terminal pending reap stayed `0`. The new tx_queue hard cap was
  active (`tx_queue_pause_high=917504`, `max_tx_queue_bytes=980698`), but
  `tun_flush_deferred` reached `446` while `send_queue_max=917482`.
- Cause: the remote-read backpressure threshold moved to the tx_queue hard cap,
  but immediate downlink flush deferral still used the soft high watermark.
  This left a second local cadence gate at the old `524288` threshold.
- Correct behavior: do not treat this as sing-box, iperf3, stale pool, QUIC, or
  close/reap loss. Before the next behavior edit, propose a TDD patch that keeps
  app pending strict but aligns tx_queue-only egress flush deferral with the
  tx_queue hard cap.

## 2026-07-06 - Hard-cap no-pending flush headroom is too permissive

- Symptom: Knife14by reduced `tun_flush_deferred` from `446` to `25`, but
  reverse-first P1 only improved to `30.0/29.0 Mbit/s` and still failed as
  `throughput_shape=low_average`.
- Evidence: direct `.27/.33/.77` baselines were healthy, `.33` current-window
  TUIC auth was clean, QUIC loss/congestion/blocking deltas were zero, app
  pending and pending-at-close were zero, and terminal pending reap stayed
  zero. The new signal was `tun_tx_dropped_delta=37` while tx_queue pressure
  reached `975399B` against `tx_queue_pause_high=917504B`.
- Cause: aligning no-pending flush deferral with the tx_queue hard cap removed
  one cadence bottleneck but allowed the local TUN/qdisc side to be pushed past
  its clean capacity.
- Correct behavior: do not simply widen immediate flush headroom again and do
  not revert to external-service hypotheses. Before the next behavior edit,
  propose a TDD patch that uses a middle or drop-aware no-pending flush guard:
  above the old soft high, below the hard cap under saturation evidence, while
  preserving strict app-pending semantics and the existing tx_queue read
  headroom.

## 2026-07-06 - Static midpoint no-pending flush threshold was not enough

- Symptom: Knife14bz bounded no-pending flush at `tx_queue_flush_high=720896B`,
  but reverse-first P1 regressed to `20.7/19.5 Mbit/s`.
- Evidence: `tun_flush_deferred=162`, `tun_tx_dropped_delta=340`,
  `max_tx_queue_bytes=983022`, `send_queue_max=917489`, and
  `throughput_shape=low_average`. App pending stayed `0`, pending-at-close and
  terminal pending reap stayed `0`, QUIC loss/congestion/blocking stayed `0`,
  and `.33` current-window TUIC auth was clean.
- Cause: the threshold took effect, but the system can still accept enough
  remote payload in bursts to overshoot the clean local TUN egress capacity
  before the remote-read pause/flush cadence catches up.
- Correct behavior: stop threshold-only tuning. The next behavior patch should
  add TDD for bounded remote payload acceptance by remaining egress headroom,
  with explicit counters for headroom-limited accepts, before another VPS run.

## 2026-07-06 - .27 cannot rely on one-shot SSH git pull

- Symptom: syncing `.27` with `git pull git@github.com:S7245/mini_vpn.git`
  failed with `Permission denied (publickey)` while the local Mac one-shot SSH
  push path worked.
- Cause: `.27` did not have GitHub SSH authentication for that pull path.
- Correct behavior: on `.27`, use its existing HTTPS fetch/tracking state and
  fast-forward from `origin/codex/knife14d-downlink-reap-open`, or configure
  deploy-key access deliberately outside the test path. Do not change the repo
  origin or retry interactive HTTPS pushes during Knife14 acceptance.

## 2026-07-06 - Cargo test accepts one filter per invocation

- Symptom: while running Knife14ca focused tests, commands such as
  `cargo test --lib test_a test_b` failed with
  `unexpected argument 'test_b' found`.
- Cause: `cargo test` accepts at most one test filter before `--`; additional
  positional filters are interpreted as invalid arguments.
- Correct behavior: run exact test filters in separate commands, or use one
  broader substring filter that matches the desired group. Do not combine
  multiple exact test names in one `cargo test` invocation.

## 2026-07-06 - Fixed pre-send headroom caps can starve receive progress

- Symptom: Knife14ca commit `82003b8` capped `send_queue_max` at `720896B`,
  but VPS reverse-first P1 regressed to `16.1/14.8 Mbit/s`.
- Evidence: final close showed `pending=566509`, `may_recv_false=11897`,
  `headroom_limited_calls=12973`, and
  `headroom_deferred_bytes=3317103134` with `tcp_state=CloseWait`,
  `can_send=true`, and `send_queue=720896`.
- Cause: a fixed queue-occupancy cap can become sticky under saturation. It
  prevents overshoot above the cap, but it does not prove the local TUN path is
  actually draining; remote accept becomes threshold-clocked rather than
  egress-progress-clocked.
- Correct behavior: do not repair this by only moving cap values. Before the
  next behavior patch, propose and test an algorithm that resumes remote
  acceptance based on observed local egress progress plus a bounded per-loop
  quantum.

## 2026-07-06 - Low-RTT summary can miss final lifecycle/drop lines

- Symptom: the Knife14ca low-RTT attribution summary reported
  `pending_at_close=0` and `tun_drops=0`, but suite-level lines after that
  summary showed `pending=566509` and TUN egress feedback
  `drop_delta_total=2691`.
- Cause: the parser summary was generated before the final post-P1
  close/drop snapshots were emitted.
- Correct behavior: parse the whole returned bundle, not just the low-RTT
  attribution block, before declaring pending, close, reap, or TUN-drop
  accounting clean. Add parser/self-test coverage for final post-summary
  lifecycle and egress feedback lines before the next expensive VPS run.

## 2026-07-06 - Test-only helpers must be cfg(test) before clippy gates

- Symptom: during Knife14cb local gates, `cargo clippy --all-targets
  --features harness -- -D warnings` failed because
  `bounded_downlink_flush_limit_for_window` became production-dead after the
  egress-clock implementation replaced its runtime use.
- Cause: the helper was still compiled into the library even though only tests
  used it.
- Correct behavior: when a behavior patch replaces a runtime helper but keeps
  it for regression tests, mark that helper `#[cfg(test)]` before running
  clippy with `-D warnings`.

## 2026-07-06 - High throughput can still hide hard-edge TUN drops

- Symptom: Knife14cb commit `5bf60d9` restored reverse-first P1 to
  `152/152 Mbit/s`, but the same bundle still showed
  `tun_tx_dropped_delta=712`, final TUN egress feedback
  `drop_delta_total=712`, and a two-second iperf zero-throughput gap.
- Evidence: close/reap accounting was clean (`pending=0`,
  `final_pending_at_close=0`, `terminal_pending_reap_bytes=0`), QUIC
  loss/congestion/blocking was zero, and current-window `.33` TUIC evidence
  had no `fail auth`.
- Cause: progress-clocked accept fixed receive-window starvation, but credit
  spending can still drive local pressure to the hard tx_queue pause threshold
  (`917504B`) and trigger kernel/TUN egress drops.
- Correct behavior: do not declare Knife14 acceptance from average throughput
  alone. Require final TUN egress/drop summaries to stay clean, and repair this
  as a credit-spend/drop-feedback algorithm issue instead of another static
  threshold tune.

## 2026-07-06 - Knife14cc pool=1 no-data was stream starvation, not hard-edge credit

- Stage: Knife14cc VPS acceptance and repeat for commit `7d1f47f`.
- Failed bundles:
  `/tmp/mini_vpn/knife14cc_hard_edge_guard_20260706_1606/mvpn_knife14cc_hard_edge_guard_usclient_suite_20260707_000531.tar.gz`
  and
  `/tmp/mini_vpn/knife14cc_repeat_20260706_1611/mvpn_knife14cc_repeat_usclient_suite_20260707_001035.tar.gz`.
- Symptom: reverse-first P1 collapsed twice with `throughput_shape=no_data`
  (`0.033 Mbit/s` and `0.034 Mbit/s` receiver) even though direct
  `.27/.33 -> .77` baselines were healthy.
- Important discriminator: TUN drops, QUIC loss/congestion/blocking,
  send-slice errors, pending-at-close, terminal pending reap, and hard-edge
  guard activity were all clean or zero. The repeat run's data stream had
  `first_rx_ms=17880` and only about `258 KiB` received.
- Cause: concurrent TUIC TCP streams on the same QUIC connection can starve the
  reverse data stream under this acceptance shape. Pool=2 A/B changed the
  stream set to `conns=0,1` and restored immediate data delivery.
- Correct behavior: make pool=2 the default isolation layer and keep pool=1 as
  an explicit A/B/regression setting. Do not keep patching local egress, close,
  or reap logic from pool=1 no-data evidence.

## 2026-07-06 - Relay writer flush is not a TUIC causality claim on quinn 0.10

- Stage: Knife14cd relay writer semantic TDD.
- Symptom: a generic `AsyncWrite` test proved a flush-gated stream can withhold
  remote response until `flush()` is called after `write_all`.
- Transport check: `quinn 0.10.2` implements `SendStream::poll_flush` as
  immediate `Poll::Ready(Ok(()))` for both `futures_io` and Tokio
  `AsyncWrite`.
- Correct behavior: keep `writer.flush().await` as a generic AsyncWrite
  semantic safeguard, but do not attribute TUIC VPS throughput changes to it.
  For current TUIC acceptance, use TCP pool isolation and stream metrics as the
  causal evidence.

## 2026-07-06 - Default pool=2 can restore throughput while worsening TUN drops

- Stage: Knife14cd default pool=2 VPS acceptance for commit `f219044`.
- Bundle:
  `/tmp/mini_vpn/knife14cd_default_pool2_20260707_0028/mvpn_knife14cd_default_pool2_usclient_suite_20260707_002800.tar.gz`
- Symptom: reverse-first P1 reached `180/179 Mbit/s` and
  `throughput_shape=stable_high`, but final egress was not clean:
  `tun_tx_dropped_delta=6754`, `drop_events=8`, `max_delta=1723`,
  `final_egress_at_close=892928`, and final send-capable pending `7680B`.
- Important discriminator: data stream starvation was gone (`conns=0,1`,
  `data_first_rx_max_ms=3`, `data_rx_bytes_max=674859752`), QUIC loss and
  congestion were zero, and terminal pending reap stayed zero.
- Cause: restoring data volume exposes the remaining local downlink egress
  limiter. Drain credit and hard-edge guard still allow repeated pressure at
  `892928B`, followed by TUN drop feedback.
- Correct behavior: do not call Knife14 done from throughput alone, and do not
  increase pool size. The next repair should add TDD for drop-aware credit debt
  or credit freeze after TUN egress drops, preserving bounded pending and final
  lifecycle visibility.

## 2026-07-06 - .27 VPS image does not have ripgrep

- Stage: Knife14cd `.27` preflight after deploying `f219044`.
- Symptom: a remote preflight command failed with `bash: rg: command not found`
  after the useful environment checks had already printed.
- Correct behavior: on `.27`, use POSIX tools such as `grep`, `find`, and
  `sed` for remote smoke/preflight checks unless `rg` is deliberately
  installed. Keep using local `rg` on the Mac workspace.

## 2026-07-06 - Exit-to-target preflight needs explicit SSH env when server evidence is off

- Stage: Knife14cj first VPS run.
- Failed bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_20260707_0229/mvpn_knife14cj_pre_payload_ack_drain_usclient_suite_20260707_022926.tar.gz`
- Symptom: the suite failed before tunnel P1 with
  `EXIT_TO_TARGET_IPERF_CHECK=1 需要设置 EXIT_SSH_HOST`.
- Cause: `EXIT_TO_TARGET_IPERF_CHECK=1` was enabled while `EXIT_SSH_HOST` and
  `TARGET_SSH_HOST` were not passed explicitly; the script did not infer them
  in that configuration.
- Correct behavior: when running `.27` suites with Exit↔Target preflight,
  always pass `EXIT_SSH_HOST=ubuntu@43.153.32.33`,
  `EXIT_SSH_KEY=/home/ubuntu/.ssh/vpn`,
  `TARGET_SSH_HOST=ubuntu@43.130.32.77`, and
  `TARGET_SSH_KEY=/home/ubuntu/.ssh/vpn`.

## 2026-07-06 - Do not use repo-wide cargo fmt as a Knife14 gate

- Stage: Knife14cj local review.
- Symptom: `cargo fmt --check` reported broad rustfmt changes across
  unrelated files, including files not touched by the stage.
- Cause: the current repository is not rustfmt-clean as a whole.
- Correct behavior: avoid running `cargo fmt` for Knife14 hot-path patches
  because it creates unrelated churn. Use `git diff --check`, clippy, focused
  tests, and small manual formatting for touched hunks unless a dedicated
  formatting-only stage is opened.

## 2026-07-06 - MTU-derived pressure ACK drain budget can be too small

- Stage: Knife14cj VPS retry.
- Bundle:
  `/tmp/mini_vpn/knife14cj_pre_payload_ack_drain_retry_20260707_0231/mvpn_knife14cj_pre_payload_ack_drain_retry_usclient_suite_20260707_023111.tar.gz`
- Symptom: pre-payload and maintenance TUN RX drains engaged, but every drain
  exhausted its budget (`attempts=252`, `packets=5292`,
  `budget_exhausted=252`, `would_block=0`) while local TUN egress drops still
  reached `7794`.
- Cause: `guard_bytes / tun_mtu` estimates data-sized packets, but pressure
  TUN RX work is mostly small TCP ACK/window packets.
- Correct behavior: derive pressure-drain packet budget from ACK-sized packets
  with a hard cap, and prove it can reach `would_block` under pressure without
  becoming unconditional TUN RX polling.

## 2026-07-06 - rsync mtime preservation can leave stale cargo artifacts on .27

- Stage: Knife14cm A/B and restore on `.27`.
- Symptom: after restoring the current `src/client_tun.rs`, the remote file
  hash and source content were correct, but `cargo test pressure_credit --lib`
  initially ran the old focused test set because cargo reused prior build
  artifacts.
- Cause: the deployment copy preserved mtimes, so cargo did not see the source
  file as newer than the artifact.
- Correct behavior: when swapping Rust source files on `.27` for A/B or VPS
  validation, force rebuild freshness with `touch src/client_tun.rs`, avoid
  mtime-preserving sync for edited core files, or clean the affected target
  artifact before trusting test counts or release binaries.

## 2026-07-06 - Pressure debt without receive gating is too late to recover

- Stage: Knife14cm valid VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14cm_pressure_credit_edge_valid_20260706_1916/mvpn_knife14cm_pressure_credit_edge_valid_usclient_suite_20260707_031619.tar.gz`
- Symptom: `pressure_credit_edge` debt installed at `pending=9074`, but remote
  payload continued after debt was active, `accepted_bytes=0` accumulated,
  pending grew to `541802`, TUN drops reached `4056`, and
  `pressure_credit_debt_paid_bytes=0`.
- Cause: credit debt limited future extra write credit, but it did not feed
  the downlink receive-window/backpressure decision. The remote reader could
  continue adding useful bytes while local egress was already pinned.
- Correct behavior: the next repair should make active drop/pressure debt part
  of receive gating. Do not keep moving thresholds or only changing debt
  sizing without proving receive pause/resume and debt repayment.

## 2026-07-06 - .27 VPS suite must source repo .env explicitly

- Stage: Knife14cn first VPS launch.
- Failed bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_20260706_1927/mvpn_knife14c_usclient_suite_20260707_032726.tar.gz`
- Symptom: the suite exited before tunnel startup with missing TUIC variables:
  `MINI_VPN_TUIC_SERVER`, `MINI_VPN_TUIC_UUID`,
  `MINI_VPN_TUIC_PASSWORD`, `MINI_VPN_TUIC_SNI`,
  `MINI_VPN_TUIC_CA_PATH`, and `MINI_VPN_TUIC_ALPN`.
- Cause: the SSH command launched the suite without loading
  `/home/ubuntu/mini_vpn/.env`; the file exists and uses `export KEY=...`
  lines.
- Correct behavior: run `.27` acceptance commands from the repo root with
  `. ./.env` before invoking the suite. Continue redacting values; only inspect
  key names or lengths when diagnosing env loading.

## 2026-07-06 - Debt-coupled receive gate does not restore throughput by itself

- Stage: Knife14cn valid VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14cn_debt_receive_gate_env_20260706_1930/mvpn_knife14c_usclient_suite_20260707_032833.tar.gz`
- Symptom: active debt paused receive at `pending=2035`, but reverse-first P1
  still stayed low at `20.5/19.2 Mbit/s` and TUN egress still dropped at the
  end.
- Cause: the previous pending-growth failure is repaired, but most of the
  30-second window still has long remote-read gaps with pending near zero and
  little TUN RX drain before pressure. The remaining bottleneck is likely
  progress cadence/ACK-window feedback before the credit edge, not more debt
  accounting.
- Correct behavior: the next repair should add a bounded, observable
  active-flow ACK/window drain path before pressure. Do not lower static
  thresholds or keep installing more debt without proving sender progress.

## 2026-07-07 - Single high pool=1 run was not stable evidence

- Stage: Knife14dk/dl/dm discriminator sequence.
- Symptom: one pool=1 run reached `166/165 Mbit/s`, but the same current code
  and default pool=1 later fell to `23.4/22.4 Mbit/s` and then near no-data.
- Cause: the single high run was a transient acceptance sample, not a stable
  causal fix. Pool size alone did not explain the remaining failure.
- Correct behavior: do not change product defaults or acceptance status from a
  single high VPS run. Require repeat evidence and clean parsed surfaces before
  treating a pool/config discriminator as causal.

## 2026-07-07 - Sing-box restart did not restore stable high throughput

- Stage: Knife14dp/dq after `.33` sing-box restart.
- Symptom: `.33` restart succeeded and direct `.33 <-> .77` baselines stayed
  healthy, but d85 reached only `20.8/19.9 Mbit/s` and current code only
  `30.9/29.9 Mbit/s`.
- Cause: the failure is not just stale sing-box service state, target iperf3,
  `.33 -> .77` TCP path, time sync, or TUIC auth. Current evidence points to
  TUIC stream read cadence on the tunnel path.
- Correct behavior: after restart, still parse the mini_vpn bundle before
  editing. If pending/close/reap/TUN/QUIC surfaces are clean and stream gaps
  remain second-scale, move to relay/TUIC read-wakeup tests instead of more
  service restarts.

## 2026-07-07 - No-op waker polling is a risky ready-drain pattern

- Stage: Knife14dq code review after stream-gap evidence.
- Symptom: current code contains `drain_ready_remote_reads`, which polls the
  split relay reader with a no-op waker to pull extra ready chunks after a real
  async read. The failing bundle still shows `tuic_stream_pending` and
  `tuic-tcp-stream-read-gap` in the `1.7-3.4s` range.
- Cause: if a manual ready-drain poll reaches `Pending`, async IO
  implementations may store that no-op waker as the current wake target. The
  relay task can then wait for a timer tick or unrelated event before polling
  again, producing bursty reads even with no local pressure.
- Correct behavior: before the next patch, add a deterministic waker-safety
  test and then either replace the ready-drain helper with a real-waker-safe
  pattern or remove the speculative ready-drain path.

## 2026-07-07 - Relay batch cap without egress credit still overdrives TUN

- Stage: Knife14dx reverse-first VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14dx_relay_credit_p1_30/mvpn_knife14dx_relay_credit_p1_30_usclient_suite_20260707_125810.tar.gz`
- Symptom: reverse-first P1 stayed low at `25.7/24.8 Mbit/s`, with repeated
  zero-throughput intervals.
- Rejected explanations: close/reap/pending accounting was clean, QUIC
  loss/congestion/blocking was clean, `.33` logs showed no current `fail auth`,
  and the relay -> main-loop channel never approached capacity.
- Cause: the relay read-side batch cap was only tied to `mpsc` channel
  occupancy. The actual pressure was downstream in smoltcp/TUN egress:
  `tun_tx_dropped_delta=591`, `send_queue_max=892928`, and
  `headroom_deferred_bytes=28883630`.
- Correct behavior: read credit must be fed from per-flow egress/send-queue
  state back to the relay reader. Do not treat a clean `global_rx_queue` as
  proof that it is safe to keep pulling full 512KiB ready batches.

## 2026-07-07 - Post-accept read credit is one batch late

- Stage: Knife14dy reverse-first VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/knife14dy_read_credit_p1_30/mvpn_knife14dy_read_credit_p1_30_usclient_suite_20260707_131037.tar.gz`
- Symptom: the data relay showed read-credit updates and 64KiB minimum batch
  limits, but reverse-first P1 still failed at `22.8/21.6 Mbit/s`.
- Important discriminator: read-credit paused only at the tail
  (`read_credit_pause_updates=1`) after local pressure already hit
  `send_queue_max=892928` and TUN egress drops appeared.
- Cause: credit was computed from actual `send_queue`/`pending` after the
  current remote payload had entered the main loop. The current batch could
  still spend stale drain-credit before the relay reader saw the pressure.
- Correct behavior: compute read-credit and pressure debt from projected local
  pressure (`send_queue + pending + incoming`) before accepting/flushing the
  payload. Do not treat a working credit channel as sufficient if its policy is
  one payload behind.

## 2026-07-07 - Pressure debt must not hard-pause QUIC stream reads

- Stage: Knife14dz projected-credit VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14dz_projected_credit_p1_30_usclient_suite_20260707_132432.tar.gz`
- Symptom: reverse-first P1 regressed to `6.05 Mbit/s` receiver and timed out
  while `read_credit_pause_updates=2` and `max_rx_blocked_stream_delta=1`
  appeared.
- Cause: projected local pressure was installed correctly, but active pressure
  debt was fed into the relay-read hard pause. This starved QUIC stream receive
  progress instead of merely bounding smoltcp/TUN flush.
- Correct behavior: use pressure debt to constrain local flush/close credit,
  not to stop relay reads. Relay reads should drain QUIC into bounded staging
  until receive-window high water or TUN drop feedback requires a real pause.

## 2026-07-07 - Do not kill remote mini_vpn with broad pkill patterns

- Stage: Knife14ea remote cleanup around `.27`.
- Symptom: a broad cleanup command matching `mini_vpn client-tun` could match
  its own SSH command line and disrupt the control session.
- Cause: process-name cleanup was expressed as a loose full-command pattern.
- Correct behavior: prefer suite-managed cleanup or narrow `pgrep`/`pkill`
  patterns that cannot match the SSH wrapper command. Verify with a non-killing
  process list before using `pkill` on VPS hosts.

## 2026-07-07 - Sing-box auth transient was service state, not config/time

- Stage: Knife14ea startup after Knife14dz.
- Symptom: `client-tun` startup failed at `tuic auth finish: sending stopped by
  peer: error 0`, while `.33` had no matching current TUIC inbound log.
- Checks: `.27` env and `.33` sing-box config matched by key length and fields,
  all hosts had synchronized time, and no TUIC `fail auth` appeared in the
  current `.33` window.
- Cause: likely transient sing-box/TUIC service state. Restarting sing-box
  restored primary startup. The auxiliary TUIC pool slot still can fail first
  auth once and recover through retry.
- Correct behavior: when this exact auth-finish/no-server-log pattern appears,
  verify env/config/time once, then inspect or restart sing-box before
  changing mini_vpn auth code.

## 2026-07-07 - Patch the intended ACK drain path, not the adjacent one

- Stage: Knife14eb local patch.
- Symptom: the first attempt to make relay read gaps use a larger ACK drain
  budget changed deferred remote-payload ACK drain instead. Focused
  `relay_gap_hint` and `tun_rx_drain_budget` tests caught the mismatch.
- Cause: similarly named budget helpers sit next to each other:
  `tun_rx_drain_budget_for_deferred_ack_drain` and
  `tun_rx_drain_budget_for_relay_gap_hint`.
- Correct behavior: for cadence fixes, add or update focused tests that name
  the exact source (`relay_gap_hint`, `remote_payload_deferred`, etc.) before
  accepting a budget change.

## 2026-07-07 - Relay-gap follow-up did not trigger in VPS

- Stage: Knife14ec reverse-first VPS acceptance.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14ec_gap_followup_p1_30_usclient_suite_20260707_135859.tar.gz`
- Symptom: the relay-gap follow-up implementation passed local tests but the
  VPS run fell to `23.2/21.5 Mbit/s` and recorded
  `relay_gap_hint_followup_attempts=0`.
- Cause: the exhaustion evidence was aggregate. The drains that exhausted
  budget were not relay-gap drains; the active/deferred remote-payload drain
  path still hit `budget_exhausted=210` with
  `remote_payload_deferred_attempts=812`.
- Correct behavior: after aggregate drain exhaustion, add source-specific
  diagnostics or attach escalation to the source that actually exhausts. The
  next patch should make deferred ACK drain escalate from active-flow budget to
  pressure budget only after it really exhausts.

## 2026-07-07 - Remote cargo needs explicit environment in non-login SSH

- Stage: Knife14ec `.27` focused tests.
- Symptom: `ssh ... cargo test` failed with `cargo: command not found`; a
  second attempt accidentally expanded `$HOME` on the Mac and looked for
  `/Users/liushan/.cargo/env` on `.27`.
- Cause: non-login SSH shells do not load `.cargo/env`, and double-quoted
  local commands expand `$HOME` before SSH.
- Correct behavior: run remote cargo commands with single-quoted SSH payloads
  and source the remote cargo environment inside the remote shell:
  `. "$HOME/.cargo/env"`.

## 2026-07-07 - Cargo accepts only one positional test filter

- Stage: Knife14en local gates.
- Symptom: `cargo test --lib <test1> <test2> ...` failed with
  `unexpected argument`.
- Cause: Cargo accepts one positional `TESTNAME` filter before `--`; multiple
  independent filters must be run as separate commands or replaced with a
  broader shared substring.
- Correct behavior: use a shared filter such as `mtu_policy` /
  `tuic_tcp_stream_diag`, run separate focused commands, or run
  `cargo test --lib --quiet`.

## 2026-07-07 - Do not test quinn MTU config through opaque Debug output

- Stage: Knife14en MTU policy TDD.
- Symptom: the first `safe1200` test failed because `TransportConfig` Debug
  output did not include `initial_mtu`, `min_mtu`, or
  `mtu_discovery_config`.
- Cause: quinn intentionally formats parts of `TransportConfig` opaquely, so
  Debug output is not a stable public observation point for MTU behavior.
- Correct behavior: keep mini_vpn-owned MTU policy as a pure profile function
  and test that profile directly; use the quinn config build only as a smoke
  check that shared VPN flow-control settings are still installed.

## 2026-07-07 - rsync to VPS must carry the project SSH key

- Stage: Knife14en `.27` sync.
- Symptom: the first `rsync` to `.27` failed with
  `Permission denied (publickey,password)`.
- Cause: the command omitted the required SSH identity from AGENTS.md.
- Correct behavior: use `rsync -e "ssh -i ~/.ssh/vpn ..."` and keep excluding
  `.env`, `.git`, and `target` when syncing the working tree to
  `/home/ubuntu/mini_vpn`.

## 2026-07-07 - TUN-MTU-derived controller tests need production-scale pressure config

- Stage: Knife14eo TDD.
- Symptom: the first `downlink_credit_controller_grows_credit_from_observed_egress_progress`
  test still failed after the controller allowed progress-based growth.
- Cause: the test used a tiny synthetic `DownlinkBackpressureConfig`
  (`high_bytes=100`) while the pressure floor is derived from TUN MTU
  (`1200 * RELAY_REMOTE_READ_PRESSURE_MIN_PACKETS`). The staging limit was
  therefore smaller than the pressure floor, so the test asserted credit growth
  in an impossible configuration.
- Correct behavior: controller tests that involve TUN-MTU-derived floors should
  either use production-scale/default backpressure config or explicitly assert
  the staging-limit clamp. Do not infer controller failure from a synthetic
  config whose watermarks are below the minimum ACK/window drain floor.

## 2026-07-07 - Hot-path projected credit helper may need a narrow clippy allow

- Stage: Knife14 downlink credit controller follow-up.
- Symptom: `cargo clippy ... -D warnings` can fail with
  `clippy::too_many_arguments` on
  `publish_projected_relay_read_credit_for_payload`.
- Cause: this helper sits on the `client_tun.rs` main-loop hot path and passes
  several already-owned local state references to avoid broad restructuring or
  allocation while iterating on the downlink credit algorithm. This file
  already has several narrow `#[allow(clippy::too_many_arguments)]` annotations
  for equivalent hot-path helpers and diagnostic formatters.
- Correct behavior: if the helper remains a local hot-path helper and the
  focused tests prove the behavior, add a narrow
  `#[allow(clippy::too_many_arguments)]` directly on that function rather than
  performing a cosmetic argument-object refactor during the performance fix.
  Revisit the signature only after the control-loop design stabilizes.

## 2026-07-07 - mini_vpn does not support `--help` as a safe preflight

- Stage: Knife14eo 97% `.27` preflight.
- Symptom: a lightweight remote preflight using `target/release/mini_vpn --help`
  exited `101` because `main.rs` treats unknown modes as a panic and supports
  only `client-tun` and `reality-probe`.
- Cause: the binary does not implement a help mode yet; the suite also records
  this panic as a non-blocking binary snapshot.
- Correct behavior: for acceptance preflight, check binary existence with
  `test -x target/release/mini_vpn` or use a real supported mode when a runtime
  smoke is required. Do not treat the existing `--help` panic snapshot as a
  tunnel failure unless it starts blocking the suite.

## 2026-07-07 - Knife14eo controller still reacts after the TUN drop edge

- Stage: Knife14eo 97% VPS reverse-first P1.
- Symptom: the scoped safe1200 P1 completed at only `32.3/30.8 Mbit/s` and
  had `tun_tx_dropped_delta=117`, despite clean QUIC loss/congestion/blocking
  and clean close lifecycle counters.
- Cause: the per-flow credit controller reduced the huge headroom-debt spiral
  but still allowed projected payload debt to push local pressure beyond the
  egress pause edge (`send_queue_max=553848`, `pending_high=576836`) before
  the hard clamp took effect.
- Correct behavior: the next patch must add a focused TDD case for projected
  payload overrun and make pressure/drop debt predictive for the next control
  epoch. Do not rerun full VPS acceptance on the same controller without a code
  change that caps projected payload credit before `tx_queue_pause_high`.

## 2026-07-07 - Static one-MTU ACK drain floor over-throttles clean egress

- Stage: Knife14ep VPS reverse-first P1.
- Symptom: the predictive drop-edge controller removed TUN drops
  (`tun_tx_dropped_delta=0`) but throughput stayed low at `30.3/28.7 Mbit/s`,
  with repeated zero-throughput intervals, `may_recv_false=8335`, and data
  stream read/pending gaps around `3.47s`.
- Cause: shrinking relay read credit below the old pressure floor was
  necessary near the drop edge, but a static one-packet ACK/window floor is too
  small once egress is clean and making progress. It protects the edge while
  starving reverse TCP receive cadence.
- Correct behavior: do not continue by shrinking the floor further or by
  re-running the same fixed-floor controller. Keep the predictive edge cap, but
  add adaptive growth from observed clean egress progress, with bounded
  multi-MTU non-paused drain credit and fast shrink only near high water or
  no-progress pressure.
