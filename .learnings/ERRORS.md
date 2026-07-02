# Errors

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
