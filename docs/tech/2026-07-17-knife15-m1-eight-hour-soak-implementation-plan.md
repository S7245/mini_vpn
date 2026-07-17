# Knife15 M1 Eight-Hour Soak Implementation Plan

> **For agentic workers:** Implement inline in this repository with the `tdd`,
> `code-review`, `self-improving-agent`, and `git-commit` skills. Subagents are
> not authorized for this plan. Every behavior is one RED -> GREEN slice.

**Goal:** Extend the reviewed macOS target-only runner with a fail-closed,
evidence-complete eight-hour M1 action derived from the accepted M0 envelope.

**Architecture:** Preserve the M0 public behavior and factor only the workload
stage vocabulary needed by both actions. M1 owns a separate profile, evidence
directory, status, checkpoints, summary fields, and exact timeline while
reusing direction-aware result validation, child identity, health checks,
collectors, cleanup, and immutable bundle publication.

**Tech Stack:** macOS/BSD Bash, awk/sed/grep, jq, iperf3, dig, existing
`scripts/knife15-macos-soak.sh --self-test`, Rust repository gates.

---

## File Map

- Modify `scripts/knife15-macos-soak.sh`: M1 public action, profile, generic
  stage helpers, timeline, checkpoints, summary/SLO fields, stop/status help.
- Modify `scripts/knife15-macos-soak-self-test.sh`: no behavior change expected;
  it remains the external invocation gate.
- Create
  `docs/tech/2026-07-17-knife15-m1-eight-hour-soak-local-results.md`: final
  RED/GREEN, regression, review, and authority record.
- Modify `AGENTS.md`, `HANDOFF.md`, `TODO.md`, and
  `.learnings/LEARNINGS.md` only after local acceptance.

### Task 1: Lock the public M1 contract

**Files:**
- Modify: `scripts/knife15-macos-soak.sh`
- Test: `scripts/knife15-macos-soak.sh --self-test`

- [ ] Add a RED self-test requiring help to expose the M1 action and variables:

```bash
grep -Fq 'scripts/knife15-macos-soak.sh m1' <<<"$usage_text"
grep -Fq 'M1_BASELINE_DIR=' <<<"$usage_text"
grep -Fq 'M1_DIRECT_DIR=' <<<"$usage_text"
```

- [ ] Run `bash scripts/knife15-macos-soak.sh --self-test`; expect failure
  `public M1 action missing from help`.
- [ ] Add `M1_BASELINE_DIR`, `M1_DIRECT_DIR`, the `m1` usage line, and the
  baseline/direct/start/smoke/m1/status/stop workflow. Add a `m1)` dispatcher
  arm calling `run_m1_action`.
- [ ] Rerun the self-test; the public-contract slice must pass until the next
  intentional RED.
- [ ] Commit as `test(knife15): lock M1 runner contract` with the minimal
  implementation that keeps the script syntactically valid.

### Task 2: Make workload identity stage-aware

**Files:**
- Modify/Test: `scripts/knife15-macos-soak.sh`

- [ ] Add RED assertions:

```bash
workload_command_matches 'bash scripts/knife15-macos-soak.sh m0'
workload_command_matches 'bash scripts/knife15-macos-soak.sh m1'
! workload_command_matches 'bash scripts/knife15-macos-soak.sh smoke'
```

- [ ] Run self-test; expect the M1 workload command to be rejected.
- [ ] Accept exactly the `m0` and `m1` public actions while retaining the saved
  full command equality check in `workload_matches_run`. Generalize warning
  text from “M0 workload” to “soak workload”; do not weaken PID identity.
- [ ] Rerun self-test and `bash -n scripts/knife15-macos-soak.sh`.
- [ ] Commit as `refactor(knife15): share soak workload identity`.

### Task 3: Add the immutable M1 profile

**Files:**
- Modify/Test: `scripts/knife15-macos-soak.sh`

- [ ] Add a RED fixture calling `write_m1_profile` and require:

```text
schema=knife15-macos-m1-v1
total_secs=28800
steady_a_secs=7200
idle_secs=300
quiet_secs=3600
steady_b_secs=7200
churn_secs=3600
steady_c_secs=6000
final_drain_secs=300
steady_tcp_forward_bps=baseline_forward/2
quiet_tcp_forward_bps=baseline_forward/4
steady_short_forward_bps=min(baseline_forward*4/5,200000000)
churn_short_connections_per_cycle=24
```

- [ ] Run self-test; expect `write_m1_profile: command not found` or the first
  missing profile assertion.
- [ ] Implement `min_rate_bps` and `write_m1_profile`, preserving baseline
  hashes, target/DNS, UDP `1160B`, reverse observer `1024B`, and exact integer
  rate derivation.
- [ ] Add `validate_m1_formal_config` requiring `28,800/7,200/300/3,600/7,200/
  3,600/6,000/300`, `METRICS_SECS=30`, and `SAMPLE_SECS=30`.
- [ ] Prove the exact formal values pass and a one-second-short total fails.
- [ ] Commit as `feat(knife15): define immutable M1 profile`.

### Task 4: Generalize phase evidence without changing M0

**Files:**
- Modify/Test: `scripts/knife15-macos-soak.sh`

- [ ] Add RED fixtures that set:

```bash
SOAK_STAGE=m1
SOAK_LABEL=M1
SOAK_EVIDENCE_DIR=m1
SOAK_STATUS_FILE=m1.status
```

  Then run one fake phase and require `m1/cycle_001_tcp-forward.json` plus
  `m1 phase start/complete` events; also rerun the existing M0 fixture and
  require its filenames/events to remain unchanged.
- [ ] Run self-test; expect M1 output to appear incorrectly under `m0/`.
- [ ] Add stage helpers and use them in health, iperf, DNS, active-window,
  schedule-status, progress, interrupt, and resume events. Default every helper
  to M0 so existing call sites remain behavior-compatible.
- [ ] Run the self-test and compare the existing M0 command log/result counts.
- [ ] Commit as `refactor(knife15): parameterize soak evidence stage`.

### Task 5: Implement the exact M1 timeline

**Files:**
- Modify/Test: `scripts/knife15-macos-soak.sh`

- [ ] Add a compressed fake M1 profile with one-second TCP/UDP/short phases and
  segment seconds `10/1/10/1/10/1/30/10/1 = 74`, retaining the formal segment
  order. Require five active-window completions, three idle/resume completions,
  one final drain, quiet/steady/churn rate commands, and 24 churn shorts in one
  full fake churn cycle.
- [ ] Run self-test; expect `run_m1_schedule` to be missing.
- [ ] Implement `run_m1_schedule_body` with the exact formal order. Reuse the
  stage-aware phase functions and keep a single monotonically increasing cycle
  index across all windows.
- [ ] Implement `run_m1_schedule` so `m1.status` moves
  `running -> complete` or `running -> failed`, with failure stopping before
  later windows.
- [ ] Rerun self-test; inspect the fake command log to prove modes and counts.
- [ ] Commit as `feat(knife15): add exact M1 workload timeline`.

### Task 6: Add four post-drain resource checkpoints

**Files:**
- Modify/Test: `scripts/knife15-macos-soak.sh`

- [ ] Add a RED fixture for `m1_checkpoint_envelope` with four rows and require
  output containing count, first/final/max RSS, FD, thread values, and final
  Endpoint ownership. Add negative fixtures for missing checkpoint, nonzero
  ownership, RSS `>131072`, RSS growth `>32768`, FD growth `>2`, and thread
  growth `>2`.
- [ ] Run self-test; expect the checkpoint parser to be missing.
- [ ] Implement `capture_m1_checkpoint` using the newest numeric process row
  and Endpoint aggregate after `sample_once_for`. Write the header once and
  capture `idle-1`, `idle-2`, `idle-3`, `final`.
- [ ] Implement `m1_checkpoint_envelope` and `m1_checkpoint_slo` with the exact
  architecture-spec bounds.
- [ ] Rerun self-test; every negative fixture must fail closed.
- [ ] Commit as `feat(knife15): enforce M1 resource checkpoints`.

### Task 7: Extend summary and acceptance evidence

**Files:**
- Modify/Test: `scripts/knife15-macos-soak.sh`

- [ ] Add a complete fake M1 run and require summary fields:

```text
m1_status
m1_active_windows_completed
m1_cycles_completed
m1_dns_completed
m1_idle_complete
m1_resume_complete
m1_final_drain_complete
m1_timeline_evidence
m1_result_evidence
m1_dns_evidence
m1_tcp_max_sender_receiver_gap_bytes
m1_udp_max_loss_percent
m1_checkpoint_evidence
m1_slo_evidence
```

- [ ] Run self-test; expect the first M1 summary field to be missing.
- [ ] Reuse the generic result/DNS envelopes on `run_dir/m1`. Set M1 result
  PASS only for zero receiver stalls and exact result multiplicity. Set M1 SLO
  PASS only when timeline/DNS/results/checkpoints/ownership/network/sample
  coverage/log/interface/rebind/TCP-gap/UDP-loss bounds all pass.
- [ ] Add one mutation at a time for `16,777,217B` TCP gap, `3.000001%` UDP
  loss, incomplete rebind, fewer than 900 samples, and log compaction; require
  `m1_slo_evidence=MISMATCH` and `internal_failure_scan=REVIEW`.
- [ ] Rerun the complete self-test.
- [ ] Commit as `feat(knife15): summarize M1 acceptance SLOs`.

### Task 8: Add the formal M1 action and fail-closed cleanup

**Files:**
- Modify/Test: `scripts/knife15-macos-soak.sh`

- [ ] Add RED tests for selection of `M1_BASELINE_DIR` by
  `direct-discriminator`, exact M1 baseline/direct hashes, fresh-age rejection,
  missing DNS, existing M0/M1 evidence, and incomplete network controls.
- [ ] Run self-test; expect the first M1 prerequisite to be unimplemented.
- [ ] Implement `run_m1_action`: validate the formal profile, routes, baseline,
  direct evidence, controls, and tools; create `m1/`, `m1-direct/`,
  `m1-checkpoints.csv`, `m1-workload.txt`, and `m1.status`; identity-register
  the controller; run the schedule; then require final sample, health,
  ownership, controls, checkpoints, and M1 SLO PASS.
- [ ] Update `status`, interrupt, stop, and bundle messages to name the active
  stage while retaining identity-verified child termination and leaving the
  TUN running after a workload failure.
- [ ] Run self-test, syntax, and the external wrapper:

```bash
bash scripts/knife15-macos-soak.sh --self-test
bash scripts/knife15-macos-soak-self-test.sh
bash -n scripts/knife15-macos-soak.sh
```

- [ ] Commit as `feat(knife15): add formal eight-hour M1 action`.

### Task 9: Full local gates and review

**Files:**
- Create: `docs/tech/2026-07-17-knife15-m1-eight-hour-soak-local-results.md`
- Modify: project memory files after PASS

- [ ] Run shell gates:

```bash
bash scripts/knife15-macos-soak.sh --self-test
bash scripts/knife15-macos-soak-self-test.sh
bash scripts/knife14-h10d16-gate.sh --self-test
bash -n scripts/knife15-macos-soak.sh
```

- [ ] Run repository regressions:

```bash
cargo test --all-targets
cargo build --release
cargo clippy --all-targets
cargo fmt --all -- --check
```

- [ ] Run `git diff --check`, secret scan the diff, and review M0 compatibility,
  workload PID/child identity, signals, route ownership, immutable evidence,
  duration/rate math, macOS awk/sed compatibility, result direction, resource
  arithmetic, and every failure stop.
- [ ] Repair any review P0/P1 with a focused RED -> GREEN before continuing.
- [ ] Record exact gates and review in the local-results document; update
  `AGENTS.md`, `HANDOFF.md`, `TODO.md`, and `.learnings/LEARNINGS.md`.
- [ ] Commit as `docs(knife15): record M1 local acceptance` and push the branch.

## Execution Stop Position

After Task 9 passes, local M1 is ready for a user-run macOS TUN. The runner
must print one exact flow: build, self-test, preflight, fresh baseline, export
`M1_BASELINE_DIR`, direct discriminator, export `M1_DIRECT_DIR`, start, smoke,
M1, status, and stop. A real M1 failure uses status/snapshot/stop; a success
uses status/stop. No real TUN is part of this implementation plan.
