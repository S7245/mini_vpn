# Learnings

## 2026-07-05 - Knife14bg rejects TUN RX drain cadence as the clean reverse root

- Code commit: `356e2d2`
- Result doc:
  `docs/tech/2026-07-05-knife14bg-tun-rx-drain-cadence-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14bg_tun_rx_drain_usclient_suite_20260705_085637.tar.gz`
- Outcome: the bounded nonblocking TUN RX drain patch passed local gates but
  failed VPS acceptance. Clean reverse-first P1 collapsed to
  `0.315/0.025 Mbit/s`.
- Key signal: the new drain path was active in the clean window
  (`tun_rx_drain packets=25 tcp=25 errors=0`), but `downlink_backpressure`,
  terminal pending, close-time pending, TUN drops, TUN flush failures,
  `send_slice` zero/errors, global_rx pressure, and clean-window QUIC
  loss/congestion were all zero. The data stream delivered only about `92 KiB`
  and the attribution moved to `tuic_stream_read_gap+relay_remote_read_gap`.
- Baseline check: `.27 -> .77` and `.33 -> .77` direct reverse preflights were
  healthy (`280` and `283 Mbit/s` receiver), and `.33` sing-box stayed active
  with normal TUIC inbound/direct outbound log lines.
- Reusable rule: when clean reverse-first has tiny remote-to-local bytes,
  active `tun_rx_drain`, and no local downlink pressure, stop changing TUN,
  pending, close/reap, or tx-queue behavior. Add behavior-neutral TUIC stream
  gap evidence and run a scoped reverse-only A/B before the next behavior
  patch.

## 2026-07-04 - Knife14az shifts the failed reverse root before local downlink drain

- Code commit: `f21e782`
- Result doc:
  `docs/tech/2026-07-04-knife14az-local-tcp-socket-policy-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14az_tcp_policy_usclient_suite_20260704_224324.tar.gz`
- Outcome: the local smoltcp socket policy patch passed local gates but failed
  VPS acceptance. Clean reverse-first P1 regressed to `0.245/0.009 Mbit/s`,
  and the suite skipped later windows because an active relay did not clear
  within the quiet wait.
- Key signal: clean summary showed `send_queue_max=1`, pending `0`, no
  downlink backpressure edges, no terminal or close-time pending, no TUN drops,
  and no QUIC loss/congestion. The attribution was
  `reverse_sender_backpressured`, with only about `35 KiB` accepted from the
  reverse data stream during the summary.
- Baseline check: `.33 -> .77` direct forward/reverse remained healthy
  (`231/232 Mbit/s` receiver), so the failure is not a simple exit-target path
  bottleneck.
- Reusable rule: when reverse-first fails with
  `reverse_sender_backpressured` and mini_vpn receives only tiny downlink bytes,
  stop tuning local tx-queue/pending/close-drain. Instrument TUIC TCP stream
  first-byte latency and remote read gaps before the next behavior patch.

## 2026-07-04 - Knife14az locks local TCP socket policy at listener and rearm

- Code commit: this Knife14az local policy stage commit.
- Stage docs:
  `docs/tech/2026-07-04-knife14az-local-tcp-socket-policy-spec.md`,
  `docs/tech/2026-07-04-knife14az-local-tcp-socket-policy-plan.md`
- Outcome: added a single local virtual-link TCP socket policy helper that
  disables smoltcp Nagle and delayed ACK for listener sockets. The same helper
  runs again before `listen` during rearm so a reused socket cannot drift back
  to smoltcp defaults after a flow closes.
- TDD signal: the listener test first failed on `nagle_enabled()`, and the
  rearm test first failed after deliberately restoring Nagle and delayed ACK
  before `rearm_socket`. Both turned green after applying the helper in the two
  required paths.
- Verification passed: focused listener/rearm tests,
  `cargo test --lib client_tun`, low-RTT probe self-test, US-client suite
  self-test, `git diff --check`, full `cargo test`, `cargo test --features
  harness`, and `cargo clippy --all-targets --features harness -- -D warnings`.
  QUIC endpoint-bind tests were confirmed outside the restricted sandbox.
- Reusable rule: local smoltcp socket options are part of lifecycle state.
  Apply them both at first listener construction and immediately before rearm
  `listen`; otherwise later connections can silently differ from the intended
  virtual-link TCP policy.

## 2026-07-04 - Knife14ay closes the terminal-pending ambiguity

- Code commit: `67a8c46`
- Result doc:
  `docs/tech/2026-07-04-knife14ay-local-tcp-closed-transition-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ay_tcp_lifecycle_usclient_suite_20260704_222042.tar.gz`
- Outcome: the behavior-neutral lifecycle diagnostics worked. Clean
  reverse-first P1 still failed (`9.15/8.18 Mbit/s`), but the new transition
  line showed the reverse data socket moved from `Established` to `Closed` in
  `dirty_relay` with `pending=0`, `terminal_candidate=false`, and about
  `30.8 MiB` already accepted into smoltcp. The later `552440` pending bytes
  were produced after that close edge when the remote relay delivered EOF.
- Key signal: clean-window QUIC loss/congestion, TUN drops, TUN flush
  failures, deferred TUN flush, `send_slice_zero`, and `send_slice_errors`
  were all zero. The active limiter was local downlink tx-queue/receive-window
  behavior: tx-queue backpressure paused/resumed remote reads while app-owned
  pending stayed zero until the close tail.
- Reusable rule: when lifecycle transition reports `terminal_candidate=false`
  at `Established -> Closed` and terminal pending appears only afterward, stop
  changing close-drain/reap policy. Focus the next patch on smoltcp local TCP
  socket send policy and tx-queue drain rate.

## 2026-07-04 - Knife14ay makes local TCP Closed transitions observable

- Code commit: this Knife14ay stage commit.
- Stage docs:
  `docs/tech/2026-07-04-knife14ay-local-tcp-closed-transition-spec.md`,
  `docs/tech/2026-07-04-knife14ay-local-tcp-closed-transition-plan.md`
- Outcome: added behavior-neutral per-handle TCP lifecycle observations. The
  first observation seeds state, later changes in TCP state or send/receive
  capability emit `tcp-lifecycle-transition` with previous source/state,
  current state, send/recv capability, pending bytes, remote-to-local bytes,
  local FIN fields, and `terminal_candidate`. The observation is reset on
  rearm, and close/reap/backpressure/TUIC/TUN behavior is unchanged.
- Parser update: low-RTT summaries now report `tcp_lifecycle` transitions,
  closed edges, terminal candidates, max pending, max remote-to-local bytes,
  sources, and states. A terminal transition adds the
  `local_tcp_terminal_transition` attribution label.
- Verification passed: focused Rust lifecycle diagnostic test,
  `cargo test --lib client_tun`, low-RTT probe self-test, US-client suite
  self-test, shell syntax check, `cargo test`, `cargo test --features
  harness`, `cargo clippy --all-targets --features harness -- -D warnings`,
  and `git diff --check`. The full cargo tests that bind local QUIC endpoints
  were rerun outside the restricted sandbox and passed.
- Reusable rule: when terminal pending appears with tiny reverse downlink
  volume, do not infer the root from the final close line alone. Capture the
  source immediately before `Closed` so the next behavior patch can distinguish
  local peer close, relay close, dirty-pass transition, and reaper discovery.

## 2026-07-04 - Knife14ax rejects tx-queue backpressure as the clean reverse root

- Code commit: `58f847d`
- Result doc:
  `docs/tech/2026-07-04-knife14ax-tx-queue-backpressure-results.md`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14ax_tx_queue_backpressure_usclient_suite_20260704_215053.tar.gz`
- Outcome: tx-queue-aware backpressure passed local gates but failed VPS
  acceptance. Clean reverse-first P1 did not complete (`iperf3: unable to
  receive results`). The new summary fields were active but stayed zero:
  `pause_edges=0`, `max_tx_queue_bytes=0`, and `max_pressure_bytes=0`.
- Key signal: the reverse data flow only reached about `221884` remote-to-local
  bytes before the local TCP socket was reaped as
  `tcp_state=Closed active=false can_send=false`, with `41208` terminal pending
  bytes. Clean-window QUIC loss/congestion, TUN drops, TUN flush failures, and
  global_rx pressure were all zero.
- Reusable rule: when reverse fails with tiny `remote_to_global_rx_bytes` and no
  tx-queue/global_rx/TUN/QUIC pressure, stop tuning backpressure. Instrument the
  local TCP state transition into `Closed` and prove whether the local peer
  closed early, a FIN/RST was mishandled, or the relay stopped reading before
  useful downlink arrived.

## 2026-07-04 - Knife14ax keeps tx-queue pressure visible to global_rx backpressure

- Stage docs:
  `docs/tech/2026-07-04-knife14ax-tx-queue-backpressure-spec.md`,
  `docs/tech/2026-07-04-knife14ax-tx-queue-backpressure-plan.md`
- Outcome: changed downlink backpressure from app-pending-only to
  tx-queue-aware pressure. Handles that accept downlink bytes into smoltcp now
  stay dirty while their socket tx queue remains above the low watermark, and
  `global_rx_paused` uses the max of app-owned pending and smoltcp tx queue
  pressure. The low-RTT parser now reports tx-queue pressure in
  `downlink_backpressure`.
- Verification passed: focused downlink TDD red/green test,
  `cargo test --lib client_tun`, low-RTT probe self-test, US-client suite
  self-test, shell syntax checks, `cargo test`, `cargo test --features
  harness`, `cargo clippy --all-targets --features harness -- -D warnings`,
  and `git diff --check`.
- Reusable rule: if `send_slice` accepts bytes into smoltcp, app-owned
  `downlink_pending` is no longer a sufficient backpressure signal. Keep the
  handle observable until the smoltcp tx queue also drains below the resume
  watermark.

## 2026-07-04 - Knife14aw identifies smoltcp tx queue pressure as the missing feedback edge

- Code commit: `ef7364c`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14aw_send_window_globalrx_usclient_suite_20260704_204329.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14aw-send-window-globalrx-results.md`
- Outcome: the behavior-neutral send-window/global_rx diagnostics worked, but
  clean reverse-first P1 still failed at `16.4/15.0 Mbit/s`. Direct `.27` and
  `.33` baselines to `.77` were about `196-198 Mbit/s`, so the public path was
  not in the failed throughput band.
- Key signal: `pending_total=0` while `send_queue_max=1048576`,
  `send_capacity_min=max=1048576`, `global_rx_queue_used_max=258/1024`,
  `send_slice` zero/error was zero, and clean-window QUIC loss/congestion was
  zero. The close tail was explicitly terminal
  (`tcp_state=Closed active=false may_send=false`) and accounted as
  `terminal_pending_reap=524627`.
- Reusable rule: when app-owned pending is empty but smoltcp tx queue is full,
  `global_rx_paused` must treat the tx queue as downlink pressure. Otherwise the
  report can show `pending_total=0` while local TCP/TUN egress is still the
  limiter.

## 2026-07-04 - Knife14aw exposes send-window and relay queue state before changing behavior

- Stage docs:
  `docs/tech/2026-07-04-knife14aw-send-window-globalrx-spec.md`,
  `docs/tech/2026-07-04-knife14aw-send-window-globalrx-plan.md`
- Outcome: added behavior-neutral diagnostics for smoltcp downlink send-window
  state and relay `global_rx` queue occupancy. `tcp-downlink-flush` now
  reports `send_window_samples`, `send_capacity_min/max`, `send_queue_max`,
  `recv_queue_max`, `may_send_false`, `may_recv_false`,
  `no_send_streak_max`, and `no_send_pending_max`. Relay live/close lines now
  report `global_rx_queue_used_max` and `global_rx_queue_capacity`, and close
  logs include `may_send`, `may_recv`, `send_capacity`, `send_queue`, and
  `recv_queue`.
- Verification passed: focused Rust tests, `cargo test --lib client_tun`, low
  RTT probe self-test, US-client suite self-test, shell syntax checks,
  `cargo test`, `cargo test --features harness`,
  `cargo clippy --all-targets --features harness -- -D warnings`, and
  `git diff --check`.
- Reusable rule: when a large bounded channel or socket buffer can hide queueing
  without crossing a wait threshold, log occupancy/capacity directly. Wait
  counters alone are not enough to distinguish "not blocked" from "buffered but
  not yet blocked".

## 2026-07-04 - Knife14av closes the reporting gap but not the reverse throughput gap

- Code commit: `b7e10b1`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14av_post_iperf_close_tail_usclient_suite_20260704_202011.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14av-post-iperf-close-tail-results.md`
- Outcome: the post-iperf settle window worked. The clean reverse-first raw
  `tcp-handle-close pending>0` line was included in the per-probe
  `terminal_pending_reap` and `pending_at_close` summaries.
- Throughput still failed: reverse-first P1 was `11.4/10.1 Mbit/s`, with no
  clean-window QUIC loss/congestion, no local write pressure, no
  `send_slice` zero/error, no TUN flush failure, and no immediate flush
  deferral.
- Remaining blind spot: `global_rx_pressure=0` only means the relay task did
  not wait beyond the pressure threshold; it does not prove the channel had no
  backlog. `no_send_capacity` also lacks `send_capacity`, `send_queue`,
  `recv_queue`, `may_send`, and no-send streak detail.
- Reusable rule: before another close/reap or pacing change, add
  behavior-neutral smoltcp send-window and relay queue occupancy diagnostics so
  a low reverse run can show where bytes are waiting before terminal close.

## 2026-07-04 - Knife14at makes TUN egress drops runtime-observable, but not sufficient root cause

- Code commit: `e8a7c41`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14at_tun_egress_feedback_usclient_suite_20260704_191427.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14at-tun-egress-feedback-results.md`
- Outcome: the scoped reverse-first P1 window reached `185/183 Mbit/s`, so the
  Knife14as low reverse result (`22.2 Mbit/s` receiver) did not reproduce.
  The clean window had `terminal_pending_reap=0`, no relay-late-remote signal,
  no local/global write pressure, no QUIC loss/congestion deltas,
  `send_slice_zero=0`, `send_slice_errors=0`, `tun_flush_failures=0`, and
  `tun_flush_deferred=0`.
- New discriminator: runtime `tcp-tun-egress` worked and matched probe-level
  accounting in the clean reverse-first window:
  `runtime_tun_egress drop_delta_total=376` and
  `tun_tx_dropped_delta=376`.
- Corrected assumption: local TUN egress drops plus downlink backpressure are
  real, but they are not sufficient to explain low reverse throughput by
  themselves. In this run reverse throughput stayed high while those signals
  were present.
- Separate branch: forward P1 remained bad (`5.56/2.06 Mbit/s`) with
  `quic_loss_congestion+local_write_pressure+local_tun_egress_drop`, and later
  reverse windows inherited QUIC congestion but still reached `184/180 Mbit/s`.
- Reusable rule: after a runtime drop signal is observable, do not treat the
  label as causal without throughput correlation. Split reverse downlink egress
  from forward/uplink QUIC congestion before changing TUN queues, watermarks,
  downlink pacing, or close-drain logic again.

## 2026-07-04 - Knife14as separated terminal pending from active reverse bottleneck

- Code commits: `575a448`, `2205734`
- Log bundle:
  `/tmp/mini_vpn/mvpn_knife14as_terminal_pending_reversefirst2_usclient_suite_20260704_182136.tar.gz`
- Result doc:
  `docs/tech/2026-07-04-knife14as-terminal-pending-lifecycle-results.md`
- Outcome: VPS throughput still failed, but the lifecycle branch got a clear
  discriminator. In the clean reverse-first window, receiver throughput was
  `22.2 Mbit/s`, `terminal_pending_reap=0`, QUIC loss/congestion deltas were
  zero, `send_slice_zero=0`, `send_slice_errors=0`,
  `tun_flush_failures=0`, and `tun_flush_deferred=0`; the active attribution was
  `local_tun_egress_drop+local_downlink_backpressure`.
- Close-boundary signal: after the clean measurement, the same reverse flow
  closed with `terminal_pending_reap_bytes=555611` and
  `tcp_state=Closed active=false can_send=false`. That makes terminal pending a
  post-close accounting outcome, not the direct clean-window throughput root.
- Reusable rule: when terminal pending appears after a reverse throughput
  window, first ask whether it was present during the measurement summary. If
  `terminal_pending_reap=0` in-window, shift the next design to the earlier
  active egress/backpressure path instead of adding more closed-socket grace.
- Next rule: do not tune queue length, TUIC pool, iperf3, sing-box, or the old
  immediate-flush pacer again before designing local TUN egress feedback or
  product-safe shaping that ties remote downlink reads to actual local egress
  capacity.

## 2026-07-04 — Knife14as starts with terminal pending accounting, not behavior tuning

Commit `575a448` added the first Knife14as slice: design/spec docs,
behavior-neutral TCP close diagnostics, and parser accounting for terminal
pending downlink bytes. All TCP close paths now log a pre-abort smoltcp socket
snapshot, and `Closed && active=false && can_send=false` pending bytes are
reported as `terminal_pending_reap_bytes`. The low-RTT probe summary now emits
`terminal_pending_reap: events=... bytes=... max_bytes=...` and keeps backward
compatibility with older logs that only had `pending`, `tcp_state=Closed`, and
`can_send=false`.

Outcome: local gates passed (`cargo test --lib client_tun`, full
`cargo test --lib`, full `cargo test`, harness test, clippy with harness,
low-RTT probe self-test, US-client suite self-test, shell syntax checks, and
`git diff --check`). No data-plane send, pacing, close, backpressure, TUIC, or
TUN behavior was intentionally changed in this slice.

Reusable rule: when a close-boundary tail appears after low reverse throughput,
do not immediately tune around the tail. First separate "terminal and no longer
deliverable" from "still deliverable but prematurely closed or window-starved"
with explicit accounting in both the process log and acceptance summary.

## 2026-07-04 — Knife14ar falsifies egress deferral as the remaining reverse root

Commit `3baf476` was tested from `.27` with the close-safe egress pacing
default. Bundle:
`/tmp/mini_vpn/mvpn_knife14ar_close_safe_default_usclient_suite_20260704_164651.tar.gz`.
The suite proved the code change took effect: default
`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216`, startup logged that pending
backlog forces immediate flush, and the clean reverse-first attribution had
`tun_flush_deferred=0`.

The acceptance still failed. Clean reverse-first P1 reached only
`17.2/16.2 Mbit/s`, below the Knife14ap receiver result of `22.0 Mbit/s`, even
though direct `.27 -> .77` and `.33 -> .77` reverse baselines were healthy
(`271 Mbit/s` and `285 Mbit/s`). The clean reverse window had no new QUIC
loss/congestion, no TUN drops, no `send_slice` zero/error, and no TUN flush
failure. The only remaining primary attribution was local downlink
backpressure.

The decisive close-boundary signal moved: after the clean reverse window,
`dead_slot_reap` still appeared with `pending=224765`,
`pending_high=583624`, `tcp_state=Closed`, `active=false`,
`can_send=false`, and `tun_flush_deferred=0`. Later polluted reverse windows
also closed with roughly `524-534 KiB` pending and no deferral. Reusable rule:
stop modifying the egress pacer for this symptom. The next stage must model
local TCP close/drain semantics and explicitly account pending bytes at terminal
reap boundaries before another throughput acceptance run.

## 2026-07-04 — Knife14ar makes downlink egress pacing close-safe by default

Knife14aq proved that a small default remote-payload egress budget was useful
as a probe but unsafe as a product default. Knife14ar therefore keeps the
`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES` knob but changes the default from
`65536` to the maximum bounded value (`16777216`) so normal runs preserve the
old immediate-flush behavior instead of reusing the rejected pacing default.
The `.27` suite default now matches Rust so acceptance does not accidentally
override the product default.

The pacer decision now receives the current downlink pending size. It may defer
only when accepted bytes leave no app-owned backlog; any non-empty
`downlink_pending` forces an immediate `iface.poll + flush_tx` attempt, even
with a zero or exhausted budget. Close/reap diagnostics also include
`tun_flush_deferred`, so the next VPS bundle can correlate pending-at-close
with pacing decisions.

Verification passed locally with focused downlink egress tests, `client_tun`,
full `cargo test`, low-RTT probe self-test, US-client suite self-test, and
`git diff --check`. Reusable rule: when pacing an accepted downlink path,
never let the pacing gate outrank lifecycle safety. Backlog ownership and
close-boundary drain/progress must stay visible and preferred over drop-count
reduction.

## 2026-07-04 — Knife14aq rejects blunt remote-payload egress pacing as a default

Commit `212ce26` passed local tests and was run from `.27` with
`MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=65536`. Bundle:
`/tmp/mini_vpn/mvpn_knife14aq_egress_pacing_default_usclient_suite_20260704_160317.tar.gz`.
Direct baselines were healthy: `.27 -> .77` reverse receiver was
`295 Mbit/s`, and `.33 -> .77` reverse receiver was `283 Mbit/s`.

The clean reverse-first window proved the pacer was active and not a server
problem: `tun_flush_deferred=1057`, no clean QUIC loss/congestion delta,
`send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`. It did
reduce TUN egress drops versus Knife14ap (`366 -> 65`), but receiver throughput
fell from `22.0 Mbit/s` to `13.8 Mbit/s`, and downlink backpressure still
oscillated around the high watermark.

The important new discriminator is the close boundary. After the clean reverse
window, a relay was reaped with `reason=dead_slot_reap`, `pending=576827`,
`tcp_state=Closed`, `can_send=false`, and `can_recv=false`. Later reverse
windows also closed with roughly `534 KiB` pending. The likely root cause is
that skipping immediate `iface.poll + flush_tx` after remote payload acceptance
can delay local egress enough for useful pending bytes to meet local close/reap.

Reusable rule: do not ship blunt remote-payload immediate-flush deferral as the
default fix. The next design must be close-safe first: pending downlink bytes
must either drain, remain owned by a live relay, or be explicitly accounted as a
terminal loss before the relay is reaped. Pacing should move closer to the TUN
write cadence or include forced drain/progress around close boundaries.

## 2026-07-04 — Knife14aq adds observable remote-payload egress pacing

Knife14ap showed the clean reverse limiter had moved after smoltcp acceptance:
`send_slice` succeeded, TUN flush calls returned success, but Linux TUN/qdisc
still reported egress drops. Knife14aq therefore avoids another watermark or
server-side guess and adds a product-side egress pacer for TCP downlink
remote-payload events.

The stage gates immediate `iface.poll + flush_tx` after remote payload
acceptance with `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES` (default 65536 bytes
per timer interval, `0` for timer-only). Accepted bytes are still preserved in
smoltcp/downlink pending; only the immediate TUN write is deferred to the
existing timer poll/flush path when the budget is exhausted. The aggregate
`tcp-downlink-flush` diagnostic now includes `tun_flush_deferred`, and the
low-RTT probe summarizes it in `downlink_flush:`.

Verification passed locally with `cargo test`, both low-RTT and US-client suite
self-tests, and `git diff --check`. This is not yet a throughput conclusion:
the next reusable rule is to require a clean `.27` reverse-first acceptance run
before deciding whether egress pacing reduced TUN drops/backpressure or needs a
different local queue strategy.

## 2026-07-04 — Promote repeated environment traps into default operating rules

User feedback after the Knife14ap run clarified that repeated sudo/TTY and
GitHub credential failures should not be rediscovered every session. When an
environment issue has a known working path, promote it into `AGENTS.md` and use
that path directly on future runs.

Reusable rule: for `.27` suites that may need `sudo -v`, start with a true
writable TTY and enter the password only at the prompt. For Mac mini GitHub
pushes where HTTPS origin cannot read credentials, use the working SSH push URL
instead of retrying HTTPS. Do not write passwords or secrets into commands,
scripts, docs, reports, learning memory, or final summaries.

## 2026-07-04 — Knife14ap acceptance pins the remaining clean reverse limiter after smoltcp acceptance

Commit `27aa73f` ran from `.27` with the new downlink flush diagnostics active.
Bundle:
`/tmp/mini_vpn/mvpn_knife14ap_flushdiag_default_bp512_128_tty2_usclient_suite_20260704_154215.tar.gz`.
The run used `bbr`, TUIC TCP pool `1`, TUN MTU `1200`, default TUN qlen `500`,
512/128 KiB downlink watermarks, and a 256 KiB flush budget. Direct baselines
were healthy: `.27 -> .77` reverse receiver was 262 Mbit/s and `.33 -> .77`
reverse receiver was 284 Mbit/s.

The clean reverse-first tunnel window still failed throughput acceptance at
`24.2/22.0 Mbit/s`, but it gave the missing discriminator. QUIC had no
loss/congestion delta or inherited congestion, and `downlink_flush:` showed
`send_slice_calls=13301`, `accepted_bytes=72320335`, `zero=0`, `errors=0`, and
`tun_flush_failures=0`. The bad signals were instead
`local_tun_egress_drop+local_downlink_backpressure`,
`pause_edges=8/resume_edges=8`, `max_pending_bytes=584779`, and
`tun_tx_dropped_delta=366`.

Reusable rule: stop treating `send_slice` failure or TUN flush syscall failure
as the leading clean reverse hypothesis. The data is accepted by smoltcp and
flush calls return success, but the local egress path still drops after those
accepted bytes are pushed toward TUN/qdisc. The next design branch should add
local egress feedback or pacing around accepted downlink bytes, not another
blind watermark/default tweak. Post-forward reverse samples in this run were
polluted by inherited QUIC congestion and should remain secondary evidence.

## 2026-07-04 — Knife14ap makes bounded downlink flush progress visible

Knife14ao default-watermark acceptance for commit `0d0765f` did not reproduce
the earlier high no-code A/B result. The `.27` bundle
`/tmp/mini_vpn/mvpn_knife14ao_default_bp512_128_flush256_tty_usclient_suite_20260704_144755.tar.gz`
showed the new defaults were active (`high=524288B`, `low=131072B`,
`flush=262144B`) and direct paths were healthy, but clean reverse-first P1 was
only `22.5/21.4 Mbit/s`. QUIC loss/congestion remained clean in that first
window; the residual signals were local TUN egress drop and downlink
backpressure. The failure is recorded in `.learnings/ERRORS.md` so future work
does not treat 512/128 KiB as accepted.

Knife14ap therefore avoided another blind tuning pass and added a smaller
observability step. `TcpDownlinkDiag` now counts non-empty flush attempts,
`can_send=false` attempts, budget-limited attempts, zero/error `send_slice`
events, total accepted bytes, largest single acceptance, and downlink-triggered
TUN flush calls/failures. The event loop emits an aggregate
`tcp-downlink-flush` line on the regular metrics tick, and the low-RTT probe
summarizes it as `downlink_flush:` in each iperf attribution block.

Verification:
- `cargo test --lib tcp_downlink_flush`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Stage review found no data-plane behavior change: the new counters are
incremented inside existing flush branches, the new aggregate line remains
behind `MINI_VPN_TCP_DIAG`, and the parser stays compatible with old logs by
printing zero-valued `downlink_flush:` fields when no new line exists. The next
VPS run should use clean reverse-first again and read `downlink_flush:` together
with TUN drops to decide whether the next code stage is egress scheduling,
smoltcp send-capacity feedback, or a different local queue strategy.

## 2026-07-04 — Knife14ao promotes 512/128 KiB downlink watermarks as defaults

Knife14ao converted the no-code A/B result into a narrow product default
change. Rust now defaults TCP downlink backpressure to 524288 bytes high and
131072 bytes low, while keeping the existing env overrides and invalid-value
repair behavior. The US-client suite defaults were aligned so a normal `.27`
run exercises the product default instead of silently injecting the old
2097120/524280 byte values.

The TDD loop was intentionally small: first add
`downlink_backpressure_defaults_match_knife14ao_ab`, watch it fail against the
old `2097120` high watermark, then change the defaults and re-run the focused
test. The suite self-test now also checks the script-level defaults and help
text so future acceptance reports do not drift away from the Rust default.

Verification:
- `cargo test --lib downlink_backpressure_defaults_match_knife14ao_ab`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `git diff --check`

Stage review found no blocking issue: the parser still accepts explicit
deployment overrides, invalid high/low values still fall back or repair to a
valid pair, and the data path remains lossless because the change only pauses
remote reads sooner. Remaining risk is operational: the next VPS run must prove
the default is active and reaches a clean reverse-first probe window. If TUIC
startup/auth fails before app-level server logging again, treat that as a
startup boundary issue, not a throughput regression.

## 2026-07-04 — Knife14ao A/B proves lower downlink watermarks are the next code lever

The first no-code A/B for Knife14ao ran commit `246f8fa` from `.27` with
default TUN qlen 500, `MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`,
1MiB TCP socket buffers, `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`,
`MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`, and
`MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`. Bundle:
`/tmp/mini_vpn/mvpn_knife14ao_bp512_128_flush256_defaultqlen_tty_usclient_suite_20260704_142413.tar.gz`.

The clean reverse-first P1 improved from the Knife14an 23.5 Mbit/s receiver
result to 157 Mbit/s receiver. QUIC stayed clean for that first reverse window
(`max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
`inherited_conns=none`), while downlink pending was bounded near the new high
watermark (`max_pending_bytes=589570` instead of roughly 2.15 MiB). This
falsifies the idea that the low reverse throughput is primarily iperf3,
sing-box, or the VPS path: the same binary and path materially improve when the
local downlink watermarks are lowered.

This A/B is not a final acceptance. The clean reverse still reported
`local_tun_egress_drop+local_downlink_backpressure` and
`tun_tx_dropped_delta=4394`, so lower watermarks reduce backlog size and improve
throughput but do not fully eliminate local TUN egress pressure. A stricter
256/64 KiB A/B was attempted next, but it failed during TUIC startup before
app-level server logging and is recorded in `.learnings/ERRORS.md`, not as a
throughput result.

Reusable rule: the next code change should be a narrow default-watermark update
or a stronger local-egress feedback mechanism, guarded by tests and a rerun
that reaches the clean reverse-first probe. Do not keep tuning iperf3,
sing-box, or QUIC congestion control until the local watermark/TUN-drop branch
has been accepted or falsified.

## 2026-07-04 — Knife14an VPS run falsifies per-flush-only downlink pacing

Commits `03f6bdc`, `739c8c0`, and `2261aed` were tested from `.27` with the
default TUN qlen 500, `MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`,
1MiB TCP socket buffers, and `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`.
Bundle:
`/tmp/mini_vpn/mvpn_knife14an_flush256_defaultqlen_tty_usclient_suite_20260704_141008.tar.gz`.

The run confirms the implementation and harness wiring worked: the report and
startup log both show the 262144-byte flush budget, `.27 -> .77` direct reverse
was 263 Mbit/s receiver, `.33 -> .77` direct reverse was 283 Mbit/s receiver,
and `tun0` stayed at the default qlen 500. No stale TUIC TCP pool reconnects
appeared.

The product hypothesis did not pass acceptance. The clean reverse-first P1 was
24.7/23.5 Mbit/s and still attributed
`local_tun_egress_drop+local_downlink_backpressure`, with two pause/resume
cycles, `max_pending_bytes=2145742`, and `tun_tx_dropped_delta=594`. This is
not a meaningful improvement over the previous post-restart/downlink-pressure
branch, and the pending high-water stayed pinned near the 2MiB global
backpressure high watermark.

The standard forward P1 still reached 198/191 Mbit/s, but it produced
648 local-write-pressure events plus heavy QUIC loss/congestion
(`lost_bytes_delta=931106142`, `congestion_events_delta=134446`). The following
reverse was correctly labeled `inherited_quic_congestion`, validating the
Knife14am attribution fix. Later full-sweep samples were polluted by that
inherited QUIC state and should not drive a product change.

Reusable rule: bounding a single `send_slice` call is not enough when the
global downlink watermarks still allow roughly 2MiB pending bursts. The next
branch should not keep tuning the per-flush cap first; it should test lower
downlink high/low watermarks or a stronger local-egress/backpressure signal,
preferably as a no-code env A/B before changing defaults.

## 2026-07-04 — Knife14an bounds each downlink flush burst before VPS retest

Commit `03f6bdc` added a configurable TCP downlink flush budget instead of
changing sing-box, iperf3, QUIC congestion control, or the stale TUIC TCP pool
branch. The grounding signal was Knife14al's same-window bundle
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`:
reverse-first tunnel throughput was 13.7/12.4 Mbit/s with
`local_tun_egress_drop+local_downlink_backpressure`, while direct client/exit
paths and sing-box health were normal.

The implementation caps one `flush_downlink` pass to
`MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES`, defaulting to 256 KiB and accepting
4 KiB through 16 MiB. Bytes beyond the cap remain in `downlink_pending`; only
the bytes actually accepted by smoltcp are drained. The US-client suite now
records and passes the active budget so `.27` VPS reports show whether the
default or an A/B override was used.

Verification:
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Stage review found no blocking bug in the local change: dirty handles remain
dirty while pending downlink exists, parser bounds avoid accidental zero or
oversized budgets, and `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=1048576` remains
available for an approximate previous-behavior A/B. Full `cargo fmt --check`
still reports broad pre-existing formatting drift, so avoid a whole-repo
formatting churn inside throughput stages unless formatting is made a separate
task.

Reusable rule: when larger smoltcp TCP buffers improve one direction but local
TUN/qdisc drops appear in the reverse path, first bound the amount admitted into
smoltcp per local egress pass. Do not use a larger TUN queue or lower global
pending watermarks as the first product fix unless the per-flush burst bound is
falsified by a same-window VPS run.

## 2026-07-04 — Knife14am labels inherited QUIC congestion before pacing work

Commit `fec33fe` fixed the low-RTT attribution gap from Knife14al without
touching Rust data-plane behavior. The new parser rule emits
`inherited_quic_congestion` when a probe window starts with a low first cwnd and
large historical QUIC loss/congestion counters, even if the per-window deltas
stay zero.

The embedded self-test now reproduces the post-forward reverse shape from
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`:
11.5/10.7 Mbit/s reverse iperf, `cwnd=5808`,
`lost_bytes=500976100`, `congestion_events=93571`, and zero new
loss/congestion delta. Before the fix this sample reported
`no_pressure_signal`; after the fix it reports `inherited_quic_congestion` and
prints `max_start_lost_bytes`, `max_start_congestion_events`,
`min_start_cwnd`, and `inherited_conns` in the `quic:` line.

Verification:
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Reusable rule: before changing pacing or watermarks from a sequential
forward-then-reverse run, first check whether the reverse window inherited a
damaged QUIC state. A clean reverse-first/fresh-connection sample is still the
stronger signal for local downlink/TUN pressure.

## 2026-07-04 — Knife14al same-window run pins the current limiter to local downlink/TUN pressure

Commit `1ab58e3` fixed the suite cleanup self-match bug, then `.27` reran the
default-queue same-window diagnostic from `/home/ubuntu/mini_vpn/.env` with
`MINI_VPN_TUIC_CC=bbr`, `MINI_VPN_TUIC_TCP_POOL=1`, reverse-first P1 enabled,
and exit-to-target preflights required. Bundle:
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen2_usclient_suite_20260704_133130.tar.gz`.

The direct paths were healthy in the same run window: client-target reverse
preflight was 269 Mbit/s receiver, exit-target forward was 279 Mbit/s receiver,
and exit-target reverse was 295 Mbit/s receiver. Post-suite direct reverse
checks from both `.27` and `.33` also stayed around the high-200 Mbit/s range.
`.33` sing-box logs showed ordinary TUIC inbound/direct outbound opens for the
target; there was no same-window service failure signal.

The clean reverse-first tunnel probe collapsed to 13.7 Mbit/s sender and
12.4 Mbit/s receiver while reporting `downlink_backpressure` pause/resume
edges, `max_pending_bytes=2153280`, `tun_tx_dropped_delta=404`, and no QUIC
loss/congestion or flow-control blocked deltas. This is no longer an iperf3,
provider baseline, `.env`, stale TCP pool slot, or sing-box-health question. The
current product branch is client-local downlink/TUN egress pressure: the remote
iperf sender slows because TCP backpressure propagates from mini_vpn's local
delivery path back through the TUIC stream.

The standard forward P1 in the same session reached 204/191 Mbit/s but produced
761 local-write-pressure events, `tun_tx_dropped_delta=9`, and heavy QUIC
loss/congestion deltas. The following reverse P1 was therefore not a clean
reverse sample: it inherited a low-cwnd/lossy QUIC state and ran at
11.5/10.7 Mbit/s. Reusable rule: when diagnosing directional throughput, trust
reverse-first or fresh-connection samples more than a reverse probe after a
lossy forward burst, and teach attribution to flag preexisting low cwnd/large
absolute loss counters instead of only per-window deltas.

Next implementation branch: do not tune iperf3 or chase sing-box first. Design
bounded/paced local downlink flushing and/or better TUN egress backpressure
watermarks, with an attribution upgrade for inherited QUIC congestion state.

## 2026-07-04 — Knife14al suite cleanup must match the executable, not the command line

The first same-window default-queue diagnostic failed before tunnel probing:
`/tmp/mini_vpn/mvpn_knife14al_samewindow_defaultqlen_usclient_suite_20260704_132521.tar.gz`.
The old cleanup used `pgrep/pkill -f '[m]ini_vpn.*client-tun'`. When the suite
was launched from `/home/ubuntu/mini_vpn`, the current shell command line
contained both `mini_vpn` and `usclient-tunnel`, so the cleanup matched and
killed its own SSH/shell process.

Commit `1ab58e3` replaced the broad regex with a `ps` parser that matches only
processes whose `comm` is `mini_vpn` and whose argv contains an independent
`client-tun` token. The script now has `--self-test` cases for the real
`mini_vpn ... client-tun` process and for false positives such as the suite
script path, sudo/env wrapper commands before exec, `pgrep -af`, and helper
probes. Verification passed locally and on `.27`:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `git diff --check`

Reusable rule: destructive process cleanup in VPS harnesses must match the
actual executable identity plus argv tokens. Never kill by a broad `-f` regex
that can match the suite path or the SSH command used to launch it.

## 2026-07-04 — Knife14ak qlen A/B shifts the live branch back to reverse sender

Commit `b31b234` added an opt-in `TUN_TX_QUEUE_LEN` suite knob and documented the
Knife14ak A/B plan. Local verification passed:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --help | rg 'TUN_TX_QUEUE_LEN|MTU=1200'`
- `git diff --check`

The scoped `.27` VPS run with `TUN_TX_QUEUE_LEN=5000`, `MINI_VPN_TUIC_CC=bbr`,
`MINI_VPN_TUIC_TCP_POOL=1`, and reverse-first P1 produced bundle
`/tmp/mini_vpn/mvpn_knife14ak_tunqlen5000_usclient_suite_20260704_131103.tar.gz`.
The suite proved the harness knob works: `tun0` reported `qdisc fq_codel` and
`qlen 5000`, and the report recorded `tun_tx_queue_len_requested=5000` plus
`tun_tx_queue_len_actual=5000`.

The A/B result did not validate TUN queue depth as the current primary limiter.
Direct preflights were healthy (`.27 -> .77` reverse receiver 274 Mbit/s,
`.33 -> .77` reverse receiver 298 Mbit/s), but tunnel reverse P1 collapsed to
0.314 Mbit/s sender and 0.057 Mbit/s receiver. Attribution was
`reverse_sender_backpressured`: local write pressure, global RX pressure,
downlink backpressure, TUN drops, QUIC loss/congestion, and QUIC blocked-frame
deltas were all zero. The close log showed `pending=0` and only 329,254 bytes
read from the remote side.

Reusable rule: a larger TUN queue can be a useful A/B knob, but if reverse iperf
sender throughput is also low and mini_vpn local pressure is clean, stop tuning
TUN/downlink. The next branch needs paired tunnel/direct reverse sender
diagnostics and exit/server-side evidence around the exact probe window before
changing product pacing or watermarks.

## 2026-07-04 — Knife14aj keeps closed-pending reap and adds TUN drop attribution

Grounding against smoltcp `0.10.0` changed the stage design: `TcpSocket::can_send()`
is `may_send() && !tx_buffer.is_full()`, and `may_send()` is true only in
`Established` and `CloseWait`. Therefore `TcpState=Closed active=false
can_send=false` pending downlink is terminal tail, not temporary local tx-buffer
pressure. Delaying `dead_slot_reap` for that state would not deliver bytes and
risks reopening the stale-flow regression that knife14u fixed.

The safer stage move was to keep the existing lifecycle guard covered by
`reap_predicate` tests and make the suspected pressure source observable. The
low-RTT probe now samples `ip -s link show <tun>` before and after each iperf
probe, prints `tun_rx_dropped_delta` and `tun_tx_dropped_delta`, and labels
positive TX-drop deltas as `local_tun_egress_drop`. The parent suite passes the
discovered `TUN_IF` into the probe and includes `tun_drops:` in the report
summary grep.

Verification:
- `cargo test --lib reap_predicate`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `git diff --check`

Reusable rule: when a close diagnostic says `tcp_state=Closed active=false
can_send=false pending>0`, treat it as terminal cleanup unless a test proves the
socket can send again. For reverse throughput, inspect TUN/qdisc dropped deltas
before changing the pending reap predicate.

VPS follow-up on `.27` at `6b45d29` produced bundle
`/tmp/mini_vpn/mvpn_knife14aj_tun_drop_env_usclient_suite_20260704_112101.tar.gz`.
The report confirmed the new attribution survives the real suite:

- reverse-first P1: sender 183 Mbit/s, receiver 182 Mbit/s,
  `tun_tx_dropped_delta=3209`, attribution
  `local_tun_egress_drop+local_downlink_backpressure`;
- standard forward P1: sender 177 Mbit/s, receiver 164 Mbit/s,
  `tun_tx_dropped_delta=17`, attribution
  `quic_loss_congestion+local_write_pressure+local_tun_egress_drop`;
- standard reverse P1: sender 124 Mbit/s, receiver 123 Mbit/s,
  `tun_tx_dropped_delta=16661`, attribution
  `local_tun_egress_drop+local_downlink_backpressure`.

The scoped run improved sharply versus the prior post-restart 23.6 Mbit/s
receiver result, but it also proved the local TUN/qdisc drop signal is real.
The next throughput branch should treat TUN egress drops and burst pacing as
first-class, not as a side note hidden behind pending-tail reap logs.

## 2026-07-04 — Knife14ai restart gate isolates next bottleneck to downlink pending

After the pool=4 startup-only smoke reproduced `tuic auth finish: sending
stopped by peer: error 0`, `.33` sing-box was restarted. The service returned
active with `ActiveEnterTimestamp=Sat 2026-07-04 09:11:13 CST`. Re-running the
same pool=4 startup-only smoke immediately passed: all four TUIC TCP pool
connections authenticated, the log printed `TUIC TCP connection pool=4`, and
the UDP relay was ready.

The post-restart scoped suite at commit `34d5cb7` completed with bundle
`/tmp/mini_vpn/mvpn_knife14ai_pool4_after_singbox_restart_usclient_suite_20260704_091224.tar.gz`.
It ran the reverse-first P1 probe and then skipped standard P1/full sweep
because the quiet gate did not observe a fresh zero-active metrics tick. Direct
preflights stayed healthy: client-target and exit-target checks were all roughly
277-284 Mbit/s receiver.

The tunnel reverse-first P1 improved to 88.9 MBytes / 24.8 Mbit/s sender and
84.2 MBytes / 23.6 Mbit/s receiver. The attribution was
`local_downlink_backpressure`: two pause/resume edges, max pending 2,149,256
bytes, no local write pressure, no global_rx pressure, no late remote bytes, and
no QUIC loss/congestion/flow-control blocked deltas. The close tail again showed
`dead_slot_reap` on handle 1 with `tcp_state=Closed active=false can_send=false
can_recv=false` and about 2.1 MiB pending, which is not the old stale pool slot
branch.

Reusable rule: once TUIC auth is restored by restarting sing-box, stop treating
pool=4 startup as the throughput bottleneck. The next code design should focus
on how downlink pending accumulates and is reaped for inactive/closed TCP state,
and on why TUN TX drops/queueing create bursty reverse throughput despite a
clean QUIC path.

## 2026-07-03 — Knife14ai pool A/B separates iperf health from TUIC pool startup

`34d5cb7` was tested from `.27` with two focused acceptance bundles:
`/tmp/mini_vpn/mvpn_knife14ai_pool1_usclient_suite_20260703_234149.tar.gz`
and
`/tmp/mini_vpn/mvpn_knife14ai_pool4_usclient_suite_20260703_234304.tar.gz`.

The pool=1 control completed. Direct preflights were healthy on all relevant
legs, including `.27 -> .77`, `.77 -> .27`, `.33 -> .77`, and `.77 -> .33`
around 275-286 Mbit/s. The tunnel reverse-first P1 improved from the prior
Kbit/s branch to 66.4 MBytes / 18.5 Mbit/s sender and 62.8 MBytes /
17.5 Mbit/s receiver, but the attribution changed to
`local_downlink_backpressure`: pending reached 2,118,593 bytes, paused once,
resumed once, and QUIC showed no loss, congestion, or flow-control blocked
delta. This means the earlier `reverse_sender_backpressured` label was useful:
it distinguishes the prior server-side sender stall from this new local
downlink-backpressure branch.

The pool=4 experiment did not reach tunnel iperf. It failed during TUIC startup
with `tuic auth finish: sending stopped by peer: error 0`. `.33` sing-box was
still systemd-active, and `.77` iperf3 was active; `.77` journal showed the
pool=4 run only performed direct/exit-target preflights and did not receive a
tunnel iperf connection. The `.33` sing-box log window showed the preceding
pool=1 TUIC connects and a stream cancel at 23:42:38 CST, but no later pool=4
TUIC inbound line at the 23:43 startup failure.

Reusable rule: do not treat low reverse throughput as an iperf3 problem when
direct client-target and exit-target preflights are healthy. For pool A/B, add
a startup-only health gate for `MINI_VPN_TUIC_TCP_POOL>1` before comparing
throughput, and keep sing-box/TUIC service state as a first-class branch even
when systemd says the service is active.

## 2026-07-03 — Knife14ai labels reverse sender backpressure in reports

Knife14ah proved a sharper failure branch: reverse receiver throughput was low,
but the target-side iperf sender was also low, while client-side relay,
downlink, and QUIC diagnostics did not show pressure/loss. Knife14ai makes that
branch visible in the low-RTT report instead of requiring manual `.77` journal
correlation.

Changes:

- `scripts/knife14b-lowrtt-probe.sh` now parses and prints
  `iperf_sender_mbps` alongside `iperf_receiver_mbps`.
- TCP reverse probes can emit `reverse_sender_backpressured` when sender and
  receiver throughput are both low and stronger local/client pressure labels
  are absent.
- The attribution path carries a `probe_kind`, so UDP reverse probes do not get
  mislabeled by the TCP sender-backpressure heuristic.
- `scripts/knife14b-usclient-tunnel-suite.sh` includes `iperf_sender_mbps` in
  the parent suite probe summary grep.
- Added
  `docs/tech/2026-07-03-knife14ai-reverse-sender-backpressure-spec.md` and
  `docs/tech/2026-07-03-knife14ai-reverse-sender-backpressure-plan.md`.

Verification passed:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `git diff --check`

Stage review found no Rust data-plane changes and no new secret exposure. The
main review catch was that `Reverse mode` also appears in UDP iperf output; the
fix was to gate `reverse_sender_backpressured` behind `probe_kind=tcp` and add
a negative self-test.

Reusable rule: when adding attribution labels to shared probe code, pass the
probe kind explicitly instead of inferring all semantics from iperf text. Iperf
phrases such as `Reverse mode` are shared across TCP and UDP.

## 2026-07-03 — Knife14ah VPS run moved reverse failure to server-side backpressure

`4282682` was tested from `.27` with
`/tmp/mini_vpn/mvpn_knife14ah_usclient_suite_20260703_231523.tar.gz`.
The run used `.27`'s project-local `.evn` silently, `MINI_VPN_TUIC_CC=bbr`,
`MINI_VPN_TUIC_TCP_POOL=4`, and a reverse-first P1 probe.

The direct host/path preflights were healthy: `.27 -> .77` 257 Mbit/s
receiver, `.77 -> .27` 264 Mbit/s receiver, `.33 -> .77` 283 Mbit/s receiver,
and `.77 -> .33` 269 Mbit/s receiver. The tunnel reverse-first P1 was still
Kbit/s-scale: iperf sender reported 3.00 MBytes / 838 Kbit/s with 2 retries,
while the receiver reported 88.2 KBytes / 24.1 Kbit/s.

Knife14ah's new relay counters split the previous late-remote branch further.
During the iperf window, handle 1 had already read 90,312 remote bytes before
local `Finish`, with no local write pressure, no global_rx pressure, no
downlink backpressure, no QUIC loss/congestion, and no QUIC blocked-frame
delta. After local `Finish`, the same relay read another 96,624 bytes and
closed by `half_closed_idle_timeout`; `tcp-handle-close` showed `pending=0`,
`send_slice_accepted=186936`, and zero TUN flush failures.

The decisive new discriminator came from `.77` iperf3 journal for the same
time window: the target-side sender only produced 3.00 MBytes in the first
second, then 29 seconds of 0 bytes with cwnd about 432 KBytes. `.33`
sing-box logs showed both TUIC inbound/direct outbound opens to `.77:5201` at
23:15:55 CST, but no close/error detail in the INFO log. This means the next
question is no longer whether the client local loop dropped received bytes; it
is why the target TCP sender is rapidly backpressured when the path goes
through sing-box/TUIC.

Reusable rule: for reverse no-pressure runs, always correlate three views
before tuning client buffers: client relay accepted bytes, target iperf sender
timeline, and exit/sing-box connection logs. If the target sender itself stops
after a tiny burst while direct exit-target reverse is healthy, inspect
server-side TUIC stream flow-control/write pressure or protocol semantics
before changing local downlink watermarks.

## 2026-07-03 — Knife14ah labels late remote bytes after local Finish

Knife14ag's reverse-first blocker was not stale pool, local pressure, downlink
backpressure, or QUIC loss. The useful discriminator was late timing: remote
bytes appeared only after local `Finish`. Knife14ah turns that manual
correlation into structured diagnostics.

Changes:

- `RelayTaskDiag` now counts `remote_after_local_finish_bytes` and reads after
  local `Finish`, and emits those counters in `tcp-relay-live` and
  `tcp-relay-close` lines under `MINI_VPN_TCP_DIAG=1`.
- `scripts/knife14b-lowrtt-probe.sh` now summarizes
  `relay_late_remote` and emits `late_remote_after_local_finish` attribution.
- The parser supports both new explicit post-finish counters and old logs by
  diffing `remote_to_global_rx_bytes` before/after `tcp-relay-write-half-closed`.
- The parent US-client suite summary now preserves relay live/half-close/close
  lines plus `relay_late_remote`.

Verification passed:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `cargo test relay_`
- `cargo test --lib client_tun`
- `git diff --check`
- parser smoke against
  `/tmp/mini_vpn/mvpn_knife14ag_usclient_suite_20260703_225103.tar.gz`, which
  now summarizes `relay_late_remote: post_finish_bytes=43772` and attribution
  `late_remote_after_local_finish`.

Full `cargo test` was attempted and passed 251 tests, but failed the two
`quic::tests::client_endpoint_binds*` local endpoint-bind tests while
`src/quic.rs` had no diff. Record this as environment residual risk, not as
evidence against the late-remote diagnostics.

Reusable rule: when reverse-first has 0 receiver bytes plus no pressure/loss
signals, preserve and summarize half-close lifecycle lines before changing data
plane behavior. If bytes only arrive after local `Finish`, the next diagnostic
belongs at TUIC/sing-box/target stream timing, not at local queue thresholds.

## 2026-07-03 — Knife14ag VPS run split reverse silence from pressure/loss

`fbe030c` was tested from `.27` with
`/tmp/mini_vpn/mvpn_knife14ag_usclient_suite_20260703_225103.tar.gz` after
loading `.27`'s project-local `.evn` file and running the suite under
`sudo -n -E env ... bash`.

The host/path preflights were healthy: `.27 -> .77` 291 Mbit/s receiver,
`.77 -> .27` 273 Mbit/s receiver, `.33 -> .77` 297 Mbit/s receiver, and
`.77 -> .33` 282 Mbit/s receiver. The tunnel reverse-first P1 still failed:
iperf sender reported 2.62 MBytes / 733 Kbit/s, while the receiver reported
0 bytes. The new attribution summary reported `no_pressure_signal`:
no local write pressure, no global_rx pressure, no downlink backpressure,
no QUIC loss/congestion delta, and no flow-control blocked-frame delta.

The decisive discriminator is timing: during the 30s iperf window the data
relay had `remote_to_global_rx_bytes=0`, but after iperf had already finished
and the local side half-closed, the same relay later read 43,772 bytes and then
closed by `half_closed_idle_timeout`. This is not the stale pool slot branch,
and it is not enough evidence for tuning congestion control, socket buffers, or
downlink watermarks.

Reusable rule: when reverse-first yields 0 receiver bytes plus
`no_pressure_signal`, inspect server/TUIC stream lifecycle and late-arriving
remote bytes before changing local pressure/backpressure settings. The next
diagnostic should make control stream vs data stream and server-side target
connect/write timing visible.

## 2026-07-03 — Knife14ag: summarize pressure per iperf window before tuning

After `knife14af2` proved stale TCP pool reconnect acceptance, the next
throughput branch split by probe segment: forward/uplink-heavy runs showed
`tcp-local-write-pressure` plus QUIC loss/congestion, while reverse-heavy runs
showed `tcp-downlink-backpressure` with no QUIC flow-control blocked frames.

Knife14ag adds per-iperf attribution summaries to the low-RTT probe. Each run
now reports receiver Mbps, selected TCP pool connections, stale reconnects,
local write pressure, global_rx pressure, downlink backlog, QUIC loss/congestion
deltas, blocked-frame deltas, and a conservative attribution label. The parent
US-client suite surfaces these summary lines in its probe summaries.

Local verification passed:

- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- parser smoke against the extracted `knife14af2` probe/report files

Reusable rule: for throughput variance, do not tune CC, pools, buffers, or
backpressure thresholds until each iperf window has a structured attribution
summary. Treat `tuic-open-tcp` as a stream-open event, not proof of a new QUIC
connection; QUIC loss/congestion attribution should use the first stats sample
inside the window as its baseline to avoid blaming old cumulative counters on a
new iperf run.

## 2026-07-03 — Knife14af2: stale TCP pool reconnect acceptance passed

`7c683b0` was tested from `.27` with
`/tmp/mini_vpn/mvpn_knife14af2_usclient_suite_20260703_214744.tar.gz` after
restarting `.33` sing-box. The suite exited successfully. The final full reverse
P1 no longer failed with `conn=1 closed=TimedOut` or 0-byte iperf; it refreshed
the stale extra pool slot first:
`tuic-tcp-pool-reconnect conn=1 reason=stale_tcp_pool_slot`.

Observed throughput after the fix:

- reverse-first P1: 30.9 Mbit/s receiver
- standalone forward P1: 2.93 Mbit/s receiver
- standalone reverse P1: 26.7 Mbit/s receiver
- full forward P1: 191 Mbit/s receiver
- full reverse P1: 23.1 Mbit/s receiver

The stale-slot acceptance is satisfied, but the next bottleneck is now a
different branch: throughput variance with local write pressure and QUIC
loss/congestion on one TCP pool connection. Keep the next stage focused on
pressure/loss rather than reopening the stale pool diagnosis.

## 2026-07-03 — Knife14ae3: reverse throughput recovered; stale TCP pool slot remains

After fixing Exit SSH and restarting `.33` sing-box, `ebd3567` was tested with
`/tmp/mini_vpn/mvpn_knife14ae3_usclient_suite_20260703_212412.tar.gz`. The new
Exit-to-Target preflight worked and proved all direct legs were healthy:
`.27 -> .77` 283 Mbit/s receiver, `.77 -> .27` 280 Mbit/s receiver,
`.33 -> .77` 281 Mbit/s receiver, and `.77 -> .33` 275 Mbit/s receiver.

Restarting sing-box changed TUIC startup from `tuic auth finish: sending stopped
by peer` to success, so an active systemd service is not sufficient proof that
TUIC auth is healthy. For future acceptance runs, when startup fails at TUIC auth
while config matches, restart or deeper-check sing-box before changing data-plane
code.

The tunnel itself improved substantially: reverse-first P1 reached 187 Mbit/s
receiver, standalone forward/reverse P1 both reached 155 Mbit/s receiver, and
full forward P1 reached 187 Mbit/s receiver. The remaining failure is narrower:
the final full reverse selected a TCP pool slot that reported `closed=TimedOut`
and transferred 0 bytes. Follow-up: refresh stale TCP pool slots before opening
new TUIC Connect streams, but only when the selected non-primary slot has no
active/opening relay.

## 2026-07-03 — Knife14ae: Exit SSH preflight needs suite-local host-key state

The first `knife14ae` run against `70e59a0` reached the new Exit-to-Target
preflight and failed before tunnel testing:
`/tmp/mini_vpn/mvpn_knife14ae_usclient_suite_20260703_211833.tar.gz`. The
direct Client↔Target checks were healthy, but SSH from `.27` to `.33` failed
with `Host key verification failed` because the suite was running under root via
`sudo -E`.

Follow-up: make the harness pass an explicit Exit SSH host-key policy and a
suite-local known-hosts file. Reusable rule: noninteractive VPS harnesses must
not depend on whichever user's `~/.ssh/known_hosts` happens to exist.

## 2026-07-03 — Knife14ad: reverse now has bytes; missing baseline is Exit↔Target

`33659c9` was tested with
`/tmp/mini_vpn/mvpn_knife14ad_usclient_suite_20260703_183503.tar.gz`. The suite
completed and the new stream-level diagnostics worked. Client-to-target direct
baselines were healthy (`.27 -> .77` 276 Mbit/s receiver, `.77 -> .27` 268
Mbit/s receiver). Tunnel fresh reverse-first improved from the earlier Kbit/s
failure to 22.4 Mbit/s receiver, but it is still far below the direct reverse
baseline. Later reverse P1 samples stayed around 15.8-20.8 Mbit/s, while full
forward P1 reached 191 Mbit/s receiver.

The new `tcp-relay-live` lines prove reverse is no longer "server sends
nothing": the data flow accumulated tens of MB of
`remote_to_global_rx_bytes`, `tcp-global-rx-pressure` did not fire, and
client loop CPU stayed low. The remaining attribution gap is the actual tunnel
reverse pre-exit path: `.77 -> .33`. The suite had only proven `.77 -> .27`,
which is not the same route.

Knife14ad follow-up adds an optional Exit-to-Target SSH preflight to
`scripts/knife14b-usclient-tunnel-suite.sh`. Reusable rule: for a TUIC exit
proxy, direct client-target baselines are necessary but not sufficient. Reverse
tunnel attribution also needs `.33 <-> .77` direct iperf from the exit host
before changing mini_vpn data-plane code.

Stage code-review found and fixed one harness usability edge case: only require
the local `ssh` command when `CHECK_VPS_SERVICES=1` and the Exit-to-Target
preflight is enabled, because disabled service preflight will not run the SSH
check.

## 2026-07-03 — Knife14ac: fresh reverse is still dead; add stream-level diagnostics

`74d3d0a` was tested with
`/tmp/mini_vpn/mvpn_knife14ac_usclient_suite_20260703_180352.tar.gz`. The run
used `RUN_REVERSE_FIRST_P1=1`, `MINI_VPN_TUIC_TCP_POOL=4`, BBR, and 1MiB
smoltcp TCP socket buffers. Direct baselines were healthy (`.77` forward
receiver 282 Mbit/s, reverse receiver 266 Mbit/s), but the fresh tunnel reverse
P1 receiver was only 62.5 Kbit/s.

This rejects two earlier sufficient-cause branches: the 64KiB local socket
window is no longer enough to explain the failure, and forward traffic on the
same QUIC connection is not required to poison reverse because reverse-first
failed before the normal sweep. Client QUIC connection stats showed no loss,
blocked frames, or loop CPU pressure during the reverse probe. The relay for the
data flow only reached `tcp-relay-write-half-closed ... reason=local_finish` and
then remained active without a close line in the captured window.

Knife14ad therefore adds per-TUIC TCP open diagnostics and relay live counters
behind `MINI_VPN_TCP_DIAG=1`, and teaches the low-RTT probe to include those
lines in markdown summaries. Reusable rule: when reverse-first is nearly silent
while client loop/QUIC stats are idle, stop tuning buffers/CC/pool and collect
per-stream evidence that distinguishes "client received no remote bytes" from
".33/sing-box/exit did not send them."

## 2026-07-03 — Knife14ab: 1MiB TCP socket buffers helped forward but not reverse

`3af4f7c` was tested with
`/tmp/mini_vpn/mvpn_knife14ab_usclient_suite_20260703_172934.tar.gz`. The user
message mentioned `7621bc6`, but the report's `repo_commit` and git snapshot
both show `3af4f7c`.

The socket-buffer change was applied correctly: the startup log printed
`TCP socket buffers: rx=1048576B tx=1048576B`. Direct baselines were healthy
again (`.77` forward receiver 276 Mbit/s, reverse receiver 281 Mbit/s). Tunnel
forward improved materially: standalone P1 reached 190 Mbit/s, and full
P2/P4/P8 reached 192/150/163 Mbit/s receiver.

Reverse did not recover: standalone P1 was 20.7 Mbit/s, and full P1/P2/P4/P8
were 17.5/26.2/32.2/26.5 Mbit/s. `send_slice_zero` and `send_slice_errors`
remained zero, so the next attribution branch is not "keep increasing smoltcp
socket buffer." The current probe always runs forward before reverse on the same
client process/TUIC connection, so it cannot distinguish intrinsic `.33 -> .27`
reverse weakness from same-connection congestion-state pollution after forward
pressure.

Knife14ac adds a reverse-only P1 probe order and a parent-suite
`RUN_REVERSE_FIRST_P1` gate. Reusable rule: once enlarged local TCP socket
buffers are confirmed but reverse remains bursty, run a fresh reverse-first
probe and a multi-connection acceptance run before asking for server logs or
changing protocol internals.

## 2026-07-03 — Knife14aa: direct reverse is healthy; test TCP socket buffer BDP next

`7621bc6` was tested with
`/tmp/mini_vpn/mvpn_knife14aa_usclient_suite_20260703_162932.tar.gz`. The new
direct reverse preflight worked and proved `.77 -> client` is not the primary
reverse blocker: direct `iperf3 -R` reached 299 Mbit/s receiver, while tunnel
reverse was still only 8.25 Mbit/s standalone P1 and 18-27 Mbit/s in the full
sweep.

The strongest live signal moved to the client downlink path. The log showed
`tcp-downlink-backpressure` transitions, high `remote_to_global_rx_bytes`, and
pending downlink at close; QUIC `tx_blocked` stayed zero. The smoltcp TCP socket
tx buffer was still fixed at 65,535 bytes, which can cap the local
mini_vpn -> app receive leg and push extra reverse bytes into `downlink_pending`.

Knife14ab makes smoltcp TCP listener rx/tx buffer sizes configurable and teaches
the US-client suite to run high-throughput acceptance with 1MiB rx/tx buffers.
Defaults remain unchanged until VPS evidence proves the throughput/memory
tradeoff.

Reusable rule: once direct `iperf3 -R` is healthy but tunnel reverse remains low
and `downlink_pending`/backpressure grows, inspect the local smoltcp send window
before returning to VPS service state, CC, or QUIC flow-control tuning.

## 2026-07-03 — Knife14z: BBR improves forward, reverse still needs direct -R attribution

`1b6f2d8` was tested with
`/tmp/mvpn_knife14z_usclient_suite_20260703_154429.tar.gz`. The CC sweep harness
worked: Cubic and BBR ran as fresh child suites with separate reports, bundles,
and `mvpn_accept_<cc>_<timestamp>.log` files.

The result split the problem. BBR materially improved forward throughput on this
US-client path: standalone P1 reached 192 Mbit/s, full P2 reached 192 Mbit/s,
and full P4 reached 150 Mbit/s, while Cubic stayed around 1-27 Mbit/s. That
confirms Cubic/path congestion is a real forward bottleneck here. But BBR is not
a complete acceptance fix: full P8 still collapsed to 18.6 Mbit/s, and reverse
remained Kbit/s-scale for both CC variants.

The existing suite only had a direct forward `.77:5201` iperf preflight. Add a
direct `iperf3 -R` baseline before routing `.77` into the TUN so future reverse
failures can be attributed to target/path/service versus tunnel data-plane
logic.

Reusable rule: do not promote BBR to a global default from forward-only wins.
For mixed TCP/UDP VPN goals, keep CC/profile decisions explicit until forward,
reverse, P8, and UDP acceptance all have matching evidence.

## 2026-07-03 — Knife14z: run congestion-control A/B after knife14y stats

`f7deb9b` was tested with
`/tmp/mini_vpn/mvpn_knife14y_usclient_suite_20260703_143704.tar.gz`. The run
completed the full US-client tunnel suite. Service preflight was healthy:
`.33` was reachable, `.77:5201` direct iperf passed, cargo/build worked, and
TUIC startup printed the expected QUIC windows and stats.

Knife14y resolved the attribution question. During the remaining stalls,
`tx_blocked(data|stream)` stayed zero while QUIC loss/congestion climbed and
`cwnd` stayed tiny, for example `lost=75744/2058977`,
`lost_bytes=95844092`, `congestion_events=48725`, `cwnd=2904`. That makes the
current blocker a congestion/path acceptance question, not another client
flow-control or relay lifecycle guess. P1 was improved relative to earlier
Kbit/s runs, but the Cubic full sweep still showed zero-rate windows.

Knife14z adds `CC_SWEEP` to `scripts/knife14b-usclient-tunnel-suite.sh`.
`CC_SWEEP="cubic bbr"` runs each CC through a fresh child suite with isolated
client logs and probe artifacts, while the old single-run behavior remains
unchanged when `CC_SWEEP` is empty. The low-RTT probe summary now also includes
`TUIC QUIC stats`, `tcp-local-write-pressure`, and
`tcp-downlink-backpressure` lines.

Reusable rule: when QUIC stats show no flow-control blocked frames but high
loss/congestion and tiny cwnd, do a same-script Cubic/BBR A/B acceptance run
before changing relay lifecycle, buffers, or product defaults.

## 2026-07-03 — Knife14x: client windows applied, remaining stall needs QUIC stats

`ee2e83a` was tested with
`/tmp/mvpn_knife14x_usclient_suite_20260703_120013.tar.gz`. The suite reached
the full tunnel path and completed. Cargo discovery/build worked, `.33`
reachability and `.77:5201` direct iperf service passed, and the TUIC startup
log confirmed the explicit client QUIC windows:
`bidi=512 uni=4096 stream_rx=8388608B conn_rx=33554432B send=33554432B`.

The result did not recover throughput. Direct `.77` iperf receiver was about
285 Mbit/s, but tunnel forward stayed around Kbit/s to low Mbit/s and reverse
remained Kbit/s-scale or timed out. `tcp-local-write-pressure` remained active:
104 pressure lines, average wait about 5.1s, max wait about 37.8s, while
`global_rx_pressure_events` stayed zero.

Knife14y should not keep tuning client windows or relay batching. Add QUIC
connection stats (`tx_blocked`, `rx_blocked`, `cwnd`, loss, congestion events,
and window-update frame counts) so the next acceptance bundle can distinguish
peer/server flow-control from congestion/path loss.

Reusable rule: once explicit client windows are logged and long write waits
remain, treat the next step as attribution, not another throughput tweak. If
`tx_blocked(data|stream)` climbs with low loss, inspect sing-box/server
flow-control. If loss/congestion climbs or cwnd stays tiny, test congestion
control or the VPS path.

## 2026-07-03 — Knife14w2: writer coalescing exposed real QUIC stream write pressure

`95bfe47` was tested with
`/tmp/mvpn_knife14w2_usclient_suite_20260703_112033.tar.gz`. The suite reached
the full tunnel path: cargo was found at `/home/ubuntu/.cargo/bin/cargo`, `.33`
reachability and `.77:5201` direct iperf preflight passed, and the final suite
status was `COMPLETED`.

The data-plane result is still below target. Direct `.77` preflight receiver was
about 281 Mbit/s, but tunnel forward P1 was 2.83 Mbit/s receiver and reverse P1
was 314 Kbit/s receiver; later reverse P2/P4/P8 timed out. The relay writer
coalescing stage did work mechanically: forward close logs showed 64KiB-class
payloads and far fewer writer calls than the earlier MSS-sized pattern. The new
signal is `tcp-local-write-pressure`: 110 pressure lines appeared, with some
`local_write_wait_max_us` values in the multi-second to tens-of-seconds range.

Knife14x makes the client QUIC transport windows explicit: raise stream receive,
connection receive, send window, and bidi-stream allowance to VPN-sized values,
and print them at TUIC startup. This does not prove the server/path window is
fixed; it removes the client's default transport window as an ambiguous cause
and gives the next log a clearer attribution point.

Reusable rule: once writer batches are 64KiB-class and `global_rx_pressure` is
zero, treat long local writer waits as QUIC stream/connection pressure. First
make local transport windows explicit and observable; if pressure persists, test
congestion control or server-side flow-control rather than more relay lifecycle
changes.

## 2026-07-03 — Knife14w VPS bundle did not test the data plane

`1144fc2` was submitted with
`/tmp/mvpn_knife14w_usclient_suite_20260703_111144.tar.gz`. The archive
contained only the markdown report and no accept log or tunnel artifacts. The
suite ran as root, passed basic env/CA checks, then failed in the build stage
because `BUILD_RELEASE=1` could not find `cargo` in root's `PATH`.

This means the relay-writer coalescing change has not yet been exercised on the
VPS path. The next run must first reach the `.33/.77` preflight and then the
tunnel/iperf sections before its result can say anything about `1144fc2` data
plane behavior.

Knife14w suite follow-up makes cargo discovery explicit: use executable `CARGO`
when provided, then `PATH`, then common rustup paths such as
`/home/ubuntu/.cargo/bin/cargo` and `/root/.cargo/bin/cargo`. The report should
record the chosen path and cargo version.

Reusable rule: when a bundle contains only the suite report, classify it as a
harness/environment result first. Do not infer data-plane success or failure
until client logs and iperf sections exist.

## 2026-07-03 — Knife14w: forward bottleneck moved to relay writer small writes

`c90471c` was tested with
`/tmp/mvpn_knife14v_usclient_suite_20260703_103350.tar.gz`. The run validated
the knife14v reverse fix: standalone reverse P1 reached roughly 181 Mbit/s and
the full reverse P1/P2/P4/P8 sweep completed around 153-183 Mbit/s receiver.
Remaining `dead_slot_reap pending>0` lines were on `tcp_state=Closed
active=false can_send=false can_recv=false`, consistent with undeliverable local
tails rather than useful downlink loss.

The new blocker is forward. Standalone forward P1 was only 2.37 Mbit/s receiver,
and full forward P1/P2/P4/P8 stayed around 1.15-2.33 Mbit/s receiver with long
zero-rate windows. The accept log for forward P1 showed about 10.5 MB written
upstream across 8529 local writer calls, i.e. MSS-sized writes, with no
`remote_write_timeout`.

Knife14w therefore coalesces already queued `RelayCommand::Data` messages inside
`run_relay_writer` before one `write_all`, preserving the existing bounded mpsc
channel as the backpressure boundary. `RelayCommand::Finish` is consumed only as
a `finish_after` flag, so queued data is written first and the upstream write
half is then shut down in FIFO order. Relay close logs now include local writer
wait counters (`local_write_wait_max_us`, `local_write_pressure_events`) to
separate true upstream write backpressure from global_rx/downlink pressure.

Local acceptance passed: `cargo test --lib client_tun`, full `cargo test`,
`cargo clippy --all-targets -- -D warnings`, and `git diff --check`.

Reusable rule: after fixing event-loop read batching, inspect writer-side write
granularity before blaming QUIC, VPS config, or congestion control. A VPN relay
can still behave like one-MSS-per-await even when the main loop drains batches
correctly.

## 2026-07-02 — Knife14v: local Finish should wait for reverse traffic to go quiet

`a57873a` was tested with
`/tmp/mvpn_knife14c_usclient_suite_20260702_223847.tar.gz`. The filename kept
the default `knife14c` tag, but the report recorded `repo_commit: a57873a`.
Knife14u's core change worked: the remaining `dead_slot_reap pending>0` carried
`tcp_state=Closed active=false can_send=false can_recv=false`, so it was an
undeliverable local tail rather than a protected useful buffer.

The run exposed the next independent reverse-path problem. Forward P1 recovered
to 13.6 Mbit/s receiver, but reverse P1 stayed at Kbit/s scale and reverse
P2/P4/P8 timed out or transferred zero. The accept log showed
`tcp-relay-write-half-closed ... reason=local_finish` followed by tiny
`remote_to_global_rx_bytes` and `half_closed_idle_timeout`. Immediate propagation
of local `CloseWait` into upstream write-half shutdown appears to interrupt
reverse traffic on this TUIC/exit path.

Knife14v defers `RelayCommand::Finish`: first `CloseWait` records a pending
local finish, remote-to-local payload refreshes the defer deadline, and Finish is
sent only after remote traffic is quiet for `LOCAL_FINISH_DEFER_SECS`. Cleanup
remains bounded by the existing half-closed idle timeout after Finish.

Reusable rule: for reverse-heavy TCP flows, "local app has no more upload bytes"
is not the same as "safe to send upstream FIN now." Preserve the read direction
while remote data is making progress, then close the write half after a bounded
quiet window.

## 2026-07-02 — Knife14u: pending grace must require local send capability

Knife14t was tested with
`/tmp/mvpn_knife14t_usclient_suite_20260702_215901.tar.gz` on commit
`51072c8`. The bundle proved the VPS path was healthy, but tunnel throughput
regressed sharply: forward fell to low Mbit/s or below, reverse had Kbit/s-scale
windows and timeout cases, and loop active stayed mostly parked. The prior
generic pending grace reduced `dead_slot_reap pending>0` sizes, but kept
inactive stale flows alive for the grace window even when the local smoltcp
socket was no longer send-capable.

Knife14u narrows the lifecycle exception: active pending is still preserved, and
inactive pending gets progress-sensitive grace only if `TcpSocket::can_send()`
is still true. Inactive pending with `can_send=false` is undeliverable tail and
is reaped immediately. `dead_slot_reap` diagnostics now include `tcp_state`,
`active`, `can_send`, and `can_recv` so the next VPS log can separate useful
tail-loss risk from correct cleanup.

Local acceptance passed after removing accidental full-repo formatting churn:
`cargo test --lib reap_predicate -- --nocapture`, `cargo test --lib client_tun`,
full `cargo test`, `cargo clippy --all-targets -- -D warnings`, and
`git diff --check`.

Reusable rule: never protect a pending buffer solely because it exists. For TCP
downlink, the pending bytes must be tied to a local socket state that can still
accept them; otherwise a grace window turns cleanup into stale-flow interference.

## 2026-07-02 — Knife14t: pending downlink grace must be generic, not only deferred close

Knife14s was tested with
`/tmp/mvpn_knife14s_usclient_suite_20260702_180337.tar.gz`. The run proved the
previous established-uplink batching fix helped: the full forward P1/P2/P4/P8
sweep completed around 11-13 Mbit/s, reverse reached roughly 18-27 Mbit/s, and
`remote_write_timeout` stayed absent. The remaining lifecycle signal was still
bad: `dead_slot_reap` closed both `state=Relaying` and `state=Closing` slots
with non-zero `downlink_pending` while `send_slice_errors=0` and
`tun_flush_tx_failures=0`.

Root cause: knife14r protected only `pending_relay_close`. That fixed part of
the `state=Closing` path, but `state=Relaying` with pending bytes and an inactive
smoltcp socket still fell through `!active => reap` before a deferred relay close
record existed.

Knife14t makes pending progress generic. `flush_downlink` now reports accepted
bytes, `SocketCtx` tracks generic downlink pending progress, and the reap
predicate gives all non-empty pending a short progress-sensitive grace. First
observation and accepted bytes refresh the grace; pure growth does not. This
keeps tail delivery bounded without letting a stuck inactive slot live forever.

Local acceptance passed: `cargo test --lib client_tun`, full `cargo test`,
`cargo clippy --all-targets -- -D warnings`, and `git diff --check`.

Reusable rule: if a lifecycle exception is justified by "useful pending bytes
may still drain", attach the exception to the pending buffer itself, not only to
one terminal event that might arrive after another reap branch.

## 2026-07-02 — Knife14s: one payload per dirty pass looked exactly like a 5ms throughput cap

Knife14r was tested with
`/tmp/mvpn_knife14r_usclient_suite_20260702_171341.tar.gz`. Service preflight was
healthy, `remote_write_timeout` stayed absent, but forward throughput stayed in
the 1-2 Mbit/s range with long zero-bps windows. The code matched the number:
established uplink read exactly one smoltcp payload per dirty pass, so quiet
periods fell back to the 5ms timer. One MSS per 5ms is roughly the observed
forward rate.

Knife14s changes the established uplink path to drain a bounded batch per dirty
pass while preserving the old safety rule: reserve relay mpsc capacity before
reading smoltcp. If the relay channel is full, leave bytes in smoltcp so TCP
window backpressure still works.

Reusable rule: when a throughput result is close to `payload_size * tick_rate`,
look for hidden "one item per timer tick" logic before changing transport
settings. Batch hot-path work with an explicit fairness cap, not with unbounded
loops.

The suite now also records the git commit and binary checksum, and
`BUILD_RELEASE=1` builds the release binary before running. Future VPS logs must
prove which commit and binary were tested before we trust performance evidence.

## 2026-07-02 — Knife14r: deferred close grace should follow drain progress

Knife14q was tested with
`/tmp/mvpn_knife14q_usclient_suite_20260702_160853.tar.gz`. It confirmed the
knife14q uplink change: `remote_write_timeout` disappeared and reverse iperf no
longer timed out. The remaining lifecycle signal was still bad for a VPN data
plane: `dead_slot_reap` closed `state=Closing` slots with non-zero
`downlink_pending`.

The reusable rule is that a deferred relay close should not be bounded by the
age of the original close event alone. It should be bounded by lack of drain
progress: while `downlink_pending.len()` keeps decreasing, the local TCP side is
still accepting useful tail bytes and the slot should stay dirty; once pending
stops decreasing for `DEFERRED_CLOSE_PENDING_GRACE_SECS`, the slot can be hard
reaped to avoid leaks.

Local acceptance for this stage: added focused tests for progress-sensitive
reap and progress bookkeeping, then passed `cargo test --lib client_tun`,
`cargo test`, `cargo clippy --all-targets -- -D warnings`, and
`git diff --check`.

## 2026-07-02 — Knife14q: QUIC stream backpressure is not a per-chunk write failure

`480c3e8` / knife14p was tested with
`/tmp/mvpn_knife14p_usclient_suite_20260702_143215.tar.gz`. The run completed,
but it did not meet acceptance: forward P=2 timed out, reverse P=2/4/8 timed
out, reverse P=1 stayed at Kbit/s scale, and the client log still showed
`remote_write_timeout attempted_bytes=1160 timeout=5s`.

The important discriminator was that `global_rx_pressure_events=0` throughout
the run, so the new downlink high/low watermark was not the active mechanism.
The code-review finding was on the opposite direction: `run_relay_writer`
treated a pending TUIC/QUIC stream `write_all` as a hard failure after 5s. That
is wrong for a VPN data plane. A pending stream write can be normal QUIC
flow-control or path backpressure and should propagate back to the local TCP
socket through the bounded relay channel and smoltcp receive window.

Reusable rule: do not put short per-chunk deadlines around data-plane
`write_all` calls unless the deadline represents a protocol invariant. Keep
cleanup bounded at the relay/task level (`idle_timeout`, stop/shutdown abort),
not at the single payload write level.

Follow-up observation: if the next VPS run still shows weak reverse throughput,
focus on downlink/half-close/reap behavior. Knife14q only removes the confirmed
uplink `remote_write_timeout` failure mode.

## 2026-07-02 — Stage workflow now requires self-improvement summaries

Project memory is split into two layers:

1. Stable project goals and operating rules live in `AGENTS.md`, so new Codex
   sessions know mini_vpn's data-plane goal before planning or editing.
2. Stage experience, failed assumptions, test conclusions, and reusable debugging
   lessons live in `.learnings/LEARNINGS.md` or `.learnings/ERRORS.md` using the
   self-improving-agent pattern.

At the end of every meaningful stage, write a compact learning summary before
asking for the next VPS run or closing the turn. Include the stage name, commit
ID or log path when available, outcome, root cause if known, and the reusable
rule for future sessions. Promote only stable, repeated guidance into
`AGENTS.md` or `docs/tech/`; keep raw one-off noise out of memory.

## 2026-06-11 — Stage 13b: UDP over TUIC Packet rides the same authenticated QUIC connection

UDP relay now speaks **TUIC `Packet` (native QUIC datagram)** through the same sing-box exit as 13a's TCP:
Shenzhen client (tuic mode) → sing-box → `dig @1.1.1.1 example.com` returned Cloudflare's real A records,
`dig @1.1.1.1 facebook.com` a second flow, and `curl https://1.1.1.1/` still got `HTTP/2 301` (TCP
non-regression on the *same* connection). Lessons:

1. **A narrowed id space silently breaks an invariant inherited from a wider one.** `AssocTable` mirrors
   Stage-12 `FlowTable`'s "monotonic `next_id`, never reuse an id" scheme (so late datagrams can't cross
   into a new flow) — but in **u16**, `next_id` wraps after 65535 interns. The original copy assigned
   `next_id` unconditionally, so after a wrap it could `insert()` onto a *still-live* id (overwriting an
   active flow's mapping → downlink misroute) and orphan that flow's `tuple_to_id` entry (slow unbounded
   leak). `FlowTable` is safe only because u32 never wraps in practice. Fix: `alloc_id()` skips in-use ids;
   the live set (≤1024) ≪ 65536 so a free id always exists. Lesson: when you copy a table/allocator and
   shrink its key width, re-audit every "the space is so large this never happens" assumption.
2. **TUIC multiplexes TCP (bi-streams) and UDP (datagrams) over ONE authenticated connection.** sing-box
   validates auth per connection, so UDP datagrams must ride the *same* connection that sent Authenticate —
   you can't open a second anonymous QUIC connection for UDP. We funnel both `open_tcp` and `send_udp`/the
   datagram pump through a single `live_conn()` (reconnect serialized by one mutex), which is also why the
   TCP non-regression `curl` is a real test of co-existence, not a separate path.
3. **Downlink applies backpressure (`send().await`), uplink drops (`try`/count).** Dropping a DNS *response*
   on the downlink breaks `getaddrinfo`, so the datagram pump blocks rather than drops; the uplink keeps
   UDP semantics (drop + count on full / TooLarge / dead connection, self-heals on the next packet). Same
   asymmetry as Stage 12's `run_quic_pump`, reused deliberately.
4. **`@1.1.1.1` is the right UDP-relay probe precisely because it dodges the local fake-resolver.** The TUN
   only forges fake-IP answers for 198.18.0.1:53; every other `:53` goes to the relay (stage-12 D1 rule).
   So `dig @1.1.1.1` returning a *real* Cloudflare IP proves the query went out through TUIC Packet, not a
   local forgery — a self-forged answer would have returned a 198.18/15 address.

## 2026-06-10 — Stage 13a: our hand-written TUIC client interoperates with sing-box

The mini_vpn client now speaks the **TUIC v5 protocol** (ADR-0004) and was accepted by a real
**sing-box TUIC server** as the exit: Shenzhen client (tuic mode) → sing-box (US, UDP 8443) →
`curl https://1.1.1.1/` returned `HTTP/2 301` with Cloudflare's genuine TLS cert and `cf-ray …-SJC`
(US egress). Lessons:

1. **Interop is the proof of correctness.** A successful sing-box handshake validates the byte-exact
   parts that unit tests can't fully cover on their own: the Authenticate token derivation
   (`export_keying_material(label=UUID, context=password)`, 32 bytes) and the Connect/Address wire
   layout (TUIC ATYP 0x00 domain / 0x01 IPv4 / 0x02 IPv6 — different from our Stage-12 custom codes).
   We unit-tested the encoders against exact bytes, but "sing-box accepted it" is the real gate.
2. **Implementing a standard protocol > a bespoke one** for this goal: speaking TUIC means the exit is
   a mature, maintained sing-box (best experience, zero server code from us). The canonical TUIC repo is
   spec-only and the reference Rust crate is yanked, so we implemented the (stable) spec on our existing
   quinn — mature *design*, our *code*, full ecosystem interop.
3. **Dual-run keeps zero regression** while swapping the upstream: `MINI_VPN_UPSTREAM=legacy|tuic`
   (default legacy), an `Upstream` enum whose `open_tcp` returns a unified `RelayStream`, and the proven
   legacy path only *wrapped*, not modified.
4. **Concurrent sessions on one branch cause loss.** Another session committed to the same branch and
   clobbered a commit (recovered + pushed). Push after every commit and keep one writer per branch.

## 2026-06-08 — Stage 12 UDP-over-QUIC cross-machine acceptance; field-debugging lessons

UDP relay over a QUIC datagram data plane works end-to-end (Shenzhen client → US exit):
ATYP=1 (IP literal: `dig @1.1.1.1`, IP echo) and ATYP=3 (fake-IP→domain: `udp.zkwcloud.com`)
both relay; 1200-byte (QUIC-initial-sized) datagrams round-trip cold; **160 concurrent flows
= 160/160** with and without DNS. Several non-obvious lessons from the cross-machine bring-up:

1. **quinn defaults bite at three points; all needed tuning for a real long-lived data plane.**
   - `max_idle_timeout` defaults to 10s and `keep_alive_interval` to None → an idle QUIC
     connection drops every 10s and reconnects forever. Set keepalive **5s** (must be well under
     even a stale peer's 10s idle, since negotiated idle = min(both peers) — version skew between
     client and server made a 10s keepalive race the 10s boundary).
   - `initial_mtu` defaults to 1200 → `max_datagram_size` ~1162, too small for a 1200B inner
     payload (~1224 with our header), so a freshly-(re)connected data plane dropped large
     datagrams until PLPMTUD warmed. Set `initial_mtu`/`min_mtu` = **1280** (IPv6 minimum) so it
     fits cold. Note quinn's "MTU" is the UDP payload size, not the IP packet — 1280 is safe on
     real IPv4/1500 paths.
   - Both keepalive and MTU live in the **shared** transport config → BOTH ends must be rebuilt;
     a stale server silently dropped the 1200B *downlink* (1204B) as oversized. Always confirm
     `git log -1` matches on every box before trusting a cross-machine result.

2. **Per-packet `println!` on a single-threaded TUN loop is catastrophic under concurrency.**
   The device dumped the full packet bytes every recv and the whole tx_queue every transmit;
   under an 8-way burst this starved the loop, overflowed the utun buffer, and dropped UDP
   (concurrent 1/160). Removing per-packet/byte-dump logging (keep per-flow + drop/error logs)
   took it to 50/160. Logging on the data-plane hot path must be per-flow, not per-packet.

3. **Localize before fixing — a passing loopback test + a field-isolating test pin the layer.**
   A loopback integration test (one QUIC connection, 100 concurrent flows, ≥90/100) proved the
   relay/server were fine, pointing at the client TUN loop. Then an IP-literal-vs-domain field
   test split DNS contention from relay throughput.

4. **The test harness was the final bottleneck.** `ncat -k -u -e /bin/cat` is connection-oriented
   / forks per peer; our server uses one egress socket per flow, so the echo saw 160 distinct
   peers and choked (15/160). A single-socket Python echo (`recvfrom`/`sendto` loop) → 160/160.
   When a relay test underperforms, suspect the echo endpoint before the relay.

## 2026-06-02 — fake-IP end-to-end reached a GFW-blocked domain; two TUN-UDP source-address gotchas

Stage 11 fake-IP worked end-to-end: Shenzhen `curl https://www.facebook.com/` →
local fake-IP → tunnel → US exit resolves the domain → real Meta server (HTTP/2 200,
genuine `*.facebook.com` cert). First time the project bypassed local DNS poisoning.

Two non-obvious bugs, both about the **source address of replies from an AnyIP TUN**:
1. The fake DNS resolver listens on 198.18.0.1:53 via AnyIP, but a reply must carry
   src=198.18.0.1. Two things were required and BOTH were needed:
   (a) add 198.18.0.1 to the iface ip_addrs (else smoltcp can't egress that src), and
   (b) bind the UDP socket to the concrete 198.18.0.1 (NOT addr:None) — with None,
   smoltcp picks the reply source by subnet match (dst 10.0.0.1 → src 10.0.0.2), the
   OS resolver drops replies whose source != the queried server, and the symptom is
   `curl: Could not resolve host` while our log clearly shows the query arriving.
   The tx queue staying EMPTY (`发货单：[]`) was the tell that the reply never egressed.

Two follow-ups (logged in TODO):
- First TCP SYN to a freshly-allocated fake-IP can hit `connection refused` (curl does
  NOT retry on refused, unlike on timeout) — likely a race between the SYN inspector
  building the listener and the SYN being processed.
- Large HTTP/2 / multiplexed streams can fail mid-transfer with `bad decrypt` — relay
  byte-stream corruption/reordering under load; mechanism is correct (first request got
  a full 200) but high-throughput stability needs investigation.

## 2026-06-01 — Full jitter is observable and correct in the reconnect logs

Stage 10 cross-machine test (kill/restart US server) showed reconnect delays of
409/440/1554/354/5966ms — non-monotonic. That non-monotonicity is the proof that
full jitter is working: plain exponential backoff would be strictly increasing
(500/1000/2000...). The randomness is what spreads 5000 clients' reconnect
moments and prevents a thundering herd. When validating backoff, check that the
delays are NOT monotonic — a monotonic sequence means jitter is broken.

Also confirmed: infinite retry (4 failures then success) + reset-on-success
(second disconnect cycle restarts the attempt counter) + epoch increments per
reconnect (1→2→3). No panic when reconnecting while idle (0 in-flight to reset).

## 2026-06-01 — Byte-level transparent TCP relay preserves end-to-end TLS

Stage 9 cross-machine test against `https://1.1.1.1/` produced a full TLS 1.3
handshake where curl saw Cloudflare's real leaf certificate (`CN=cloudflare-dns.com`,
SSL.com intermediate) — not our dev cert. Confirms that the smoltcp + yamux + TLS
pipeline only carries opaque bytes; the inner TLS session is established between
the original client (curl) and the real target (Cloudflare), with our tunnel
acting as a pure pipe. HTTP/2 multiplexing also works end-to-end. This is the
right property for a VPN: the tunnel must NOT terminate the user's TLS or it
would break SNI / cert pinning / E2E privacy.

## 2026-05-27 — Verify worktree baseline before any design/implementation

The session worktree `claude/frosty-gates-10390f` was 39 commits behind `main`
(client_tun.rs was 133 lines vs main's 867). All early code reading was against
`main`'s files via absolute paths, so the design discussion nearly proceeded on a
stale base. Fixed with `git merge --ff-only main` (worktree HEAD was an ancestor of
main, 0 unique commits, no loss).

- Before designing/implementing in a worktree, check `git -C <worktree> log` and
  `git rev-list --count HEAD..main`. Don't assume the worktree == latest.

## 2026-05-27 — Run git in the worktree, not the main checkout

`cd /…/mini_vpn && git status` runs in the MAIN repo; the session's working tree is
the worktree under `.claude/worktrees/…`. New files written to the worktree won't show
in a `git status` run from main. Use `git -C <worktree-path>` or stay in the default
cwd (the worktree) — avoid `cd`-ing to the main checkout for git ops.

## 2026-05-29 — Always check the binary's startup banner before debugging behavior

A cross-machine test failed mysteriously: many `📡 收到` SYN logs, zero
`🎯 extracted target`, server kept reporting yamux disconnects. Root cause was
NOT code or topology — it was a stale binary. Stage 8 changes lived only on the
worktree branch, but the user was running `./target/debug/mini_vpn` built from
the main checkout (still at pre-Stage 8). The diagnostic tell was the startup
banner: it contained `target=httpbin.org:80` (a field Task 3 had removed),
proving the running binary predated Task 3.

- After ANY code change, first check the startup banner / version line matches
  the expected build. Don't debug behavior against a binary you didn't just compile.
- When work lives on a feature branch / worktree, prefer running the binary from
  that worktree's `target/debug/` until the branch is merged, or merge first.

## 2026-05-27 — Same-machine upstream + a global TUN host route = egress loop

Stage 8 round-trip test failed with the upstream `server` on localhost: the
`route add -host <target> -interface <our-utun>` is machine-global, so the
server's own `connect(<target>)` egress is ALSO diverted into our TUN instead of
reaching the real internet -> `Connection refused` / loop. Target extraction
itself worked (client logged `🎯 extracted target 1.1.1.1:80`, server logged the
correct target); only the outbound hop broke.

- The full byte round-trip can only be validated with the upstream on a
  DIFFERENT host (the real VPN topology — e.g. the US server). On a single
  machine the route inevitably captures the server's egress to the target.
- Always use an IP literal as the test target on dev machines: a co-resident
  fake-ip TUN proxy (Clash/Mihomo) hijacks DNS to 198.18.0.0/15.

Note: `67c466b add` committed the worktree directory itself as a gitlink
(mode 160000 `.claude/worktrees/frosty-gates-10390f`). Recursive/self-referential;
worth cleaning up separately.

## 2026-07-04 — Knife14au terminal-zero is not close-pending-zero

- Stage: Knife14au pending-at-close taxonomy.
- Outcome: Added behavior-neutral close pending classification to
  `tcp-handle-close` diagnostics and low-RTT probe summaries. Local gates passed:
  `cargo test --lib client_tun`, full `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, probe/suite self-tests, shell
  syntax checks, and `git diff --check`.
- Key lesson: `terminal_pending_reap: events=0 bytes=0` only proves no close
  matched `Closed && active=false && can_send=false`; it does not prove there
  were no close-time pending bytes. Knife14at raw logs had
  `tcp_state=Established active=true can_send=false pending>0`, which was
  previously visible only by manual raw-log inspection.
- Reusable rule: every reverse/downlink acceptance report must read
  `pending_at_close` alongside `terminal_pending_reap`. If throughput is low,
  split pending tails by `terminal_closed_no_send`, `active_no_send`,
  send-capable, inactive no-send, and unknown before changing close-drain,
  pacing, TUN queue length, TUIC pool, iperf3, or sing-box.

## 2026-07-04 — Knife14av per-probe summaries need a post-iperf settle window

- Stage: Knife14av post-iperf close-tail reporting.
- Outcome: Knife14au VPS acceptance showed reverse TCP throughput recovered
  (`179/179`, `185/185`, and `183/181 Mbit/s`), but one full-reverse raw
  `tcp-handle-close pending>0` line appeared after that probe's attribution
  summary had already been written.
- Key lesson: a final post-run metric tail is not enough for Knife14 branch
  decisions. If close/reap logs arrive just after `iperf3` exits, the raw log
  may contain the evidence while the per-probe `pending_at_close` attribution
  remains zero.
- Reusable rule: reverse/downlink acceptance probes must wait a short, bounded
  post-iperf settle window before sampling final TUN counters and summarizing
  metrics. Keep the window explicit in the report and configurable for quick
  local debugging.

## 2026-07-04 — Knife14ba needs first-byte/read-gap evidence before behavior patches

- Stage: Knife14ba TUIC stream first-byte and relay read-gap diagnostics.
- Outcome: Added behavior-neutral timing diagnostics for relay remote reads and
  TUIC TCP stream reads, plus low-RTT parser summaries/attribution for
  `tuic_stream_first_byte_slow`, `tuic_stream_read_gap`,
  `relay_remote_first_byte_slow`, and `relay_remote_read_gap`.
- Local gates passed: `cargo test --lib tuic::tests::`,
  `cargo test --lib client_tun::tests::`,
  `scripts/knife14b-lowrtt-probe.sh --self-test`,
  `scripts/knife14b-usclient-tunnel-suite.sh --self-test`, full `cargo test`
  outside sandbox, `cargo test --features harness` outside sandbox,
  `cargo clippy --all-targets --features harness -- -D warnings`, and
  `git diff --check`.
- Key lesson: after Knife14az showed `remote_to_global_rx_bytes` near zero with
  no downlink pending, terminal pending, TUN drop, or clean-window QUIC loss,
  the next useful evidence is stream first-byte latency and read gaps, not
  another close-drain, pacer, pool, iperf3, or sing-box tweak.
- Reusable rule: if reverse-first throughput is low and local downlink/pending
  counters stay quiet, parse `tuic_tcp_stream` and `relay_remote_timing` before
  changing behavior. A large first-byte or read-gap label should drive the next
  patch; contradictory timing evidence should trigger architecture review.

## 2026-07-04 — Knife14ba rules out TUIC first-byte for clean reverse-first

- Stage: Knife14ba VPS acceptance for stream timing diagnostics.
- Bundle: `/tmp/mini_vpn/mvpn_knife14ba_stream_timing_usclient_suite_20260704_231537.tar.gz`
- Code commit: `7e25e92`.
- Outcome: clean reverse-first P1 improved only to `31.0/28.7 Mbit/s`, but
  TUIC first receive was fast (`3ms`) and the data stream delivered about
  `110MB` into mini_vpn with `max_read_gap_ms=3763`. Clean-window QUIC
  loss/congestion, TUN drops, global_rx pressure, and local write pressure were
  all quiet.
- Key lesson: the remaining clean reverse-first root is local TCP downlink
  delivery under tx-queue / receive-window pressure, not delayed TUIC stream
  first byte. The run ended with `max_tx_queue_bytes=574824`,
  `pending_at_close=542302`, and `terminal_pending_reap=542302` on a closed
  non-send-capable local TCP socket.
- Reusable rule: after first-byte/read-gap diagnostics contradict a remote-read
  stall hypothesis, keep those diagnostics but move the next TDD slice to local
  send-queue pressure and terminal pending accounting. Do not revisit stale
  pool, iperf3, sing-box, or blunt pacing without new contradictory evidence.

## 2026-07-04 — Knife14bb parser attribution must separate control and data streams

- Stage: Knife14bb parser data-stream attribution.
- Outcome: Updated `scripts/knife14b-lowrtt-probe.sh` so global stream timing
  maxima remain visible, but reverse TCP read-gap attribution uses data-bearing
  streams/handles only. The parser now reports `data_streams`,
  `data_*_max_ms`, `data_rx_bytes_max`, and `data_rx_min_bytes=65536`.
- Local gates passed: `scripts/knife14b-lowrtt-probe.sh --self-test`,
  `scripts/knife14b-usclient-tunnel-suite.sh --self-test`, and `bash -n` for
  both scripts.
- Key lesson: a large read gap is not enough to identify the throughput root.
  It must be paired with stream byte volume; otherwise idle iperf control
  streams can create false remote-read labels.
- Reusable rule: parser attribution for throughput root cause should use
  data-stream filtered fields, while raw/global maxima remain in the report for
  manual inspection.

## 2026-07-04 — Knife14bc scales tx-queue backpressure with the configured tx buffer

- Stage: Knife14bc tx-window scaled backpressure defaults.
- Outcome: Added adaptive default downlink backpressure for runtime env config:
  when `MINI_VPN_TCP_TX_BUFFER_BYTES` is larger than the legacy `512KiB` high
  watermark and backpressure env values are not explicitly set, the high
  watermark rises to the tx buffer size and the low watermark rises to one
  quarter of that size. Explicit backpressure env values still win.
- Local gates passed: focused RED/GREEN test, `cargo test --lib
  client_tun::tests::`, low-RTT parser self-test, US-client suite self-test,
  full `cargo test` outside sandbox, `cargo test --features harness` outside
  sandbox, and `cargo clippy --all-targets --features harness -- -D warnings`.
- Key lesson: Knife14ba showed useful receiver bytes matched
  `send_slice_accepted`; the terminal pending tail arrived after local close.
  The earlier limiter is the fixed 512KiB tx-queue pause threshold acting as a
  receive-window cap on a VPS RTT path.
- Reusable rule: when TCP socket tx buffer is explicitly enlarged for
  throughput, tx-queue backpressure defaults must scale with that buffer unless
  the operator explicitly configures different watermarks.

## 2026-07-04 — Knife14bd suite must not override adaptive runtime defaults

- Stage: Knife14bd suite adaptive backpressure.
- Outcome: Updated `scripts/knife14b-usclient-tunnel-suite.sh` so default
  downlink backpressure env values are empty and reported as `<auto>`, letting
  the Knife14bc binary scale high/low from `MINI_VPN_TCP_TX_BUFFER_BYTES`.
  Explicit operator-provided high/low env values still pass through.
- Local gates passed: suite self-test and shell syntax check.
- Key lesson: code-side adaptive defaults are ineffective if acceptance scripts
  always export fixed legacy env values.
- Reusable rule: before a VPS run that validates runtime config defaults, grep
  the suite for env exports that may shadow the binary default being tested.

## 2026-07-04 — Knife14bd 1MiB adaptive backpressure is necessary but insufficient

- Stage: Knife14bd VPS acceptance for tx-buffer-scaled downlink backpressure.
- Bundle:
  `/tmp/mini_vpn/knife14bd_scaled_auto_20260704_234307/mvpn_knife14bd_scaled_auto_usclient_suite_20260704_234307.tar.gz`
- Code under test: `9578f5e`.
- Outcome: startup confirmed auto watermarks `high=1048576B low=262144B`, but
  clean reverse-first stayed at `27.8/26.2 Mbit/s`. Clean-window QUIC
  loss/congestion, TUN drops, local/global write pressure, and flush failures
  remained quiet. The data stream first read was `3ms`, data max read gap was
  `3826ms`, `send_queue_max=1048576`, and terminal pending/reap grew to
  `1058416B`.
- Key lesson: moving the high watermark from 512KiB to the configured 1MiB tx
  buffer validates the receive-window branch but does not clear it. The next
  discriminating test is a scoped tx-buffer/receive-window A/B, not another
  stale-pool, sing-box, iperf3, TUN queue, or egress-pacer change.
- Reusable rule: if terminal pending scales with the tx buffer and clean
  throughput remains burst/idle, test whether larger tx-window capacity improves
  useful receiver throughput before changing close-drain behavior again. If it
  only scales the terminal tail, pivot to local TCP/TUN drain cadence.

## 2026-07-05 — Knife14be falsifies simple receive-window capacity

- Stage: Knife14be 4MiB receive-window A/B.
- Bundle:
  `/tmp/mini_vpn/knife14be_tx4m_auto_20260705_065724/mvpn_knife14be_tx4m_auto_usclient_suite_20260705_065724.tar.gz`
- Code under test: `5f1cbfb`.
- Outcome: startup confirmed `tx=4194304B` and auto downlink backpressure
  `high=4194304B low=1048576B`, but clean reverse-first dropped to
  `20.3/18.9 Mbit/s`. Clean-window QUIC loss/congestion remained quiet, but
  TUN egress drops appeared: `tun_tx_dropped_delta=27530`,
  `runtime_tun_egress drop_delta_total=14946`.
- Key lesson: increasing the local TCP tx buffer moved pressure from
  visible 1MiB backpressure/terminal pending into TUN/qdisc loss. The root is
  not simply receive-window capacity; the next useful branch is local TCP/TUN
  drain cadence or TUN-drop-aware feedback.
- Reusable rule: if a larger tx buffer worsens clean reverse and introduces TUN
  egress drops while QUIC stays clean, stop increasing buffers. Treat the larger
  buffer as evidence that burst smoothing / TUN feedback is missing.

## 2026-07-05 — Knife14bf adds TUN-drop feedback as a separate pause reason

- Stage: Knife14bf local TUN-drop-aware downlink feedback.
- Outcome: Added a separate `TunEgressFeedbackState` so runtime TUN egress
  drops can pause `global_rx` without being counted as ordinary
  high-watermark downlink backpressure. The feedback samples Linux TUN
  `tx_dropped` once per second, pauses when a positive drop delta appears with
  local downlink pressure, and resumes once local pressure drains to the
  configured low watermark.
- Parser/result shape: `scripts/knife14b-lowrtt-probe.sh` now reports
  `tun_egress_feedback` separately from `runtime_tun_egress` and avoids
  double-counting `tcp-tun-egress-feedback` as a `tcp-tun-egress` runtime drop
  sample.
- Local gates passed: `cargo test --lib client_tun`,
  `bash scripts/knife14b-lowrtt-probe.sh --self-test`,
  `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`,
  `bash -n` for both scripts, and `git diff --check`.
- Key lesson: Knife14be's new root is not "any TUN drop is fatal"; Knife14at
  already showed high reverse throughput with some drops. The actionable shape
  is "large tx queue below high watermark + clean QUIC + TUN drops + no
  ordinary backpressure," which needs its own feedback/accounting path.
- Reusable rule: when adding a new diagnostic line with a shared prefix, tighten
  parser regexes first. Otherwise derived lines such as
  `tcp-tun-egress-feedback` can be accidentally counted as base
  `tcp-tun-egress` samples.

## 2026-07-05 — Knife14bf feedback did not hit clean reverse-first pressure

- Stage: Knife14bf VPS acceptance for TUN-drop-aware downlink feedback.
- Bundle:
  `/tmp/mini_vpn/knife14bf_tun_feedback_rerun_20260705_073541/mvpn_knife14bf_tun_feedback_rerun_usclient_suite_20260705_073541.tar.gz`
- Code under test: `e661613`.
- Outcome: after a sing-box restart cleared an initial TUIC startup failure,
  clean reverse-first P1 still measured only `22.8/21.6 Mbit/s`. The clean
  window had no QUIC loss/congestion and no TUN flush failures, but still had
  ordinary downlink backpressure (`pause_edges=7`, `max_tx_queue_bytes=1048576`)
  plus terminal pending/reap of `1108466B`.
- Key lesson: the sysfs TUN-drop feedback gate is observable and can fire on
  VPS, but it did not fire in the clean reverse-first window because the drop
  delta was sampled when `dirty_handles=0` and `pending_total=0`. The primary
  clean limiter remains local TCP/TUN drain cadence plus 1MiB tx-queue
  backpressure/terminal pending, not stale pools, server path, larger receive
  windows, or egress pacing.
- Reusable rule: treat Linux TUN drop counters as lagging diagnostics unless
  they are correlated with same-window local pressure. The next Knife14 patch
  should reduce ordinary `downlink_backpressure` and terminal pending directly,
  not only add later drop feedback.

## 2026-07-05 — Knife14bg should test TUN ingress fairness before more tuning

- Stage: Knife14bg grounding/spec for TUN RX drain cadence.
- Outcome: code reading showed remote downlink bytes enter through
  `global_rx.recv()`, while local TCP ACK/window-update packets enter only via
  `device.wait_for_rx()`, one packet at a time. The 5ms timer polls smoltcp but
  does not read new TUN packets. Under a constantly ready remote downlink
  branch, this can delay local ACK/window processing until tx-queue backpressure
  pauses remote reads.
- Key lesson: a full smoltcp tx queue with clean QUIC and zero TUN flush
  failures can be caused by ingress fairness, not just egress write capacity.
- Reusable rule: before changing buffers again, add a bounded nonblocking TUN
  RX drain path and diagnostics that prove whether local ACK/window updates are
  being processed promptly during remote downlink pressure.

## 2026-07-05 — Knife14bh shows bg TUN RX drain is a regression trigger

- Stage: Knife14bh stream-gap A/B.
- Code under test: `7e33d44`.
- Bundles:
  `/tmp/mini_vpn/mvpn_knife14bh_default_usclient_suite_20260705_094918.tar.gz`
  and
  `/tmp/mini_vpn/mvpn_knife14bh_drain0_usclient_suite_20260705_095120.tar.gz`.
- Outcome: default `MINI_VPN_TUN_RX_DRAIN_BUDGET=8` collapsed clean
  reverse-first P1 to `0.245/0.035 Mbit/s` and produced a data stream
  `data_pending_gap_max_ms=24516` with only `209758B` received. Setting
  `MINI_VPN_TUN_RX_DRAIN_BUDGET=0` restored useful flow to
  `26.5/24.5 Mbit/s`, data stream first byte `4ms`, and about `99MB` received.
- Key lesson: the Knife14bg opportunistic TUN RX drain path is not safe as a
  default. It can starve useful reverse downlink rather than improve ACK/window
  fairness.
- Reusable rule: when an opportunistic fairness path runs inside a hot remote
  payload branch, prove it with an off-switch A/B before accepting it as a
  default. If `budget=0` materially improves throughput, flip the default off
  before continuing deeper lifecycle work.

## 2026-07-05 — Drain-disabled path exposes next local limiter

- Stage: Knife14bh drain0 analysis.
- Outcome: with TUN RX drain disabled, clean reverse-first still only reached
  `24.5 Mbit/s` receiver. TUIC data-stream first byte was fast and data pending
  gap was no longer the primary label, while local pressure appeared as
  `downlink_backpressure pause_edges=11`, `send_queue_max=1048576`,
  `tun_tx_dropped_delta=1639`, and pending-at-close `5617B`
  (`active_no_send`).
- Key lesson: after removing the bg drain regression, the remaining Knife14
  branch is local TUN egress/downlink backpressure, not TUIC stream first-byte
  starvation.
- Reusable rule: separate regression removal from next-root optimization. First
  make the default match the known-better drain0 shape, then tune the recovered
  egress/backpressure limiter.

## 2026-07-05 — Knife14bi default-off patch reached VPS startup but not throughput

- Stage: Knife14bi default drain-off.
- Code under test: `76af8dc`.
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bi_default_usclient_suite_20260705_111824.tar.gz`.
- Outcome: local gates passed and `.27` startup confirmed
  `TUN RX drain budget: 0 packets/pass`, but the scoped VPS suite failed before
  throughput with `tuic auth finish: sending stopped by peer: error 0`.
  Direct `.27 -> .77` and `.33 -> .77` baselines were healthy, `.33` sing-box
  was active/listening/config-valid, certificates were valid, and a no-secret
  exact comparison showed UUID/password/SNI/ALPN all matched.
- Key lesson: this bundle is not throughput evidence and does not invalidate
  the drain default-off patch. It is the same startup-failure class observed in
  Knife14bf before sing-box restart.
- Reusable rule: when TUIC auth-finish startup fails while service health and
  exact no-secret config comparison pass, restart sing-box once and rerun the
  same scoped suite before touching mini_vpn data-plane code.
