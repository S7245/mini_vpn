# Errors

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
