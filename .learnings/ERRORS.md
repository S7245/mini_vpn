# Errors

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
