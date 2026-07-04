# Learnings

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
