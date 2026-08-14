# Knife15 M2 Market Continuity Calibration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> to implement this plan task-by-task.

**Goal:** Build a bounded macOS HITL calibration harness that compares
mini_vpn with a mature TUIC client on the same `.33/.77` path before any
established-stream architecture replacement is selected.

**Architecture:** Add an isolated calibration runner rather than changing the
frozen formal M2 runner. It consumes one immutable direct-baseline profile,
observes an operator-owned VPN client without controlling it, runs a six-cycle
mixed workload, continues through quality events, and fails closed on invalid
evidence. A small Python helper performs deterministic iperf JSON reduction;
the shell layer owns route/preflight, workload lifecycle, observer binding,
and bundling.

**Tech Stack:** POSIX-oriented Bash on macOS, Python 3 standard library,
iperf3 JSON, existing Knife15 Exit observer, route/scutil/networksetup evidence,
git exact-source metadata, tar and SHA-256.

---

## Implementation Rules

- Use the `diagnose` and `tdd` skills for every behavior change.
- Keep `scripts/knife15-macos-soak.sh` and its formal M2 contract unchanged.
- Do not copy production credentials into fixtures, commands, or artifacts.
- Commit each coherent task and push without another authorization pause.
- Stop implementation only for a non-expected regression that changes this
  plan; expected REDs proceed to the smallest GREEN.
- Do not request a Mac run until all local gates and code review pass.

### Task 1: Deterministic iperf continuity reducer

**Files:**

- Create: `scripts/knife15-market-iperf-summary.py`
- Create: `scripts/fixtures/knife15-market/tcp-two-zero-runs.json`
- Create: `scripts/fixtures/knife15-market/tcp-partial-tail.json`
- Create: `scripts/fixtures/knife15-market/udp-complete.json`
- Test: `scripts/knife15-market-iperf-summary.py --self-test`

**Step 1: Write failing fixture assertions**

Add a `--self-test` entry point that loads the three repository fixtures and
asserts this exact contract:

```python
assert tcp["receiver_zero_intervals"] == 3
assert tcp["max_consecutive_receiver_zero_intervals"] == 2
assert tcp["receiver_zero_windows"] == [[2.0, 3.0], [3.0, 4.0], [7.0, 8.0]]
assert partial["complete_receiver_intervals"] == 2
assert partial["receiver_zero_intervals"] == 0
assert udp["lost_percent"] == 1.25
```

The partial-tail fixture must contain a final interval shorter than 0.95
seconds with zero bytes. The reducer must exclude it.

**Step 2: Run the RED**

Run:

```bash
python3 scripts/knife15-market-iperf-summary.py --self-test
```

Expected: failure because the reducer is not implemented.

**Step 3: Implement the smallest reducer**

The command interface is:

```bash
python3 scripts/knife15-market-iperf-summary.py tcp --reverse 0 RESULT.json
python3 scripts/knife15-market-iperf-summary.py udp --reverse 1 RESULT.json
python3 scripts/knife15-market-iperf-summary.py --self-test
```

Emit one deterministic JSON object. Reject malformed/missing `intervals`,
iperf errors, missing end summaries, or non-numeric values with a nonzero exit.
Count only intervals whose duration is in `[0.95, 1.25]`. For TCP, choose the
receiver side by the iperf `sender` boolean and requested direction; accept an
explicit `--reverse` flag rather than inferring it from byte magnitudes.

**Step 4: Run GREEN and malformed-input checks**

Run:

```bash
python3 scripts/knife15-market-iperf-summary.py --self-test
printf '{"intervals":[]}' | \
  python3 scripts/knife15-market-iperf-summary.py tcp --reverse 0 -
```

Expected: self-test prints `knife15 market iperf summary self-test passed`;
the malformed command exits nonzero.

**Step 5: Commit**

```bash
git add scripts/knife15-market-iperf-summary.py scripts/fixtures/knife15-market
git commit -m "test(knife15): add market continuity reducer"
git push origin codex/knife14d-downlink-reap-open
```

### Task 2: Immutable load-profile creation and validation

**Files:**

- Create: `scripts/knife15-market-continuity.sh`
- Modify: `scripts/knife15-market-iperf-summary.py`
- Test: `bash scripts/knife15-market-continuity.sh --self-test`

**Step 1: Add RED self-tests**

The shell self-test creates an isolated temporary baseline containing known
forward/reverse iperf JSON and asserts that `profile` produces:

```text
schema=knife15-market-profile-v1
target=43.130.32.77
iperf_port=5201
forward_rate_bps=16000000
reverse_rate_bps=28000000
short_forward_rate_bps=25600000
short_reverse_rate_bps=44800000
cycles=6
```

It must also assert rejection of a changed Target, changed port, missing
baseline file, zero/negative rate, and a symlinked profile.

**Step 2: Run RED**

```bash
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: missing-command or missing-implementation failure.

**Step 3: Implement profile ownership**

Support:

```bash
bash scripts/knife15-market-continuity.sh profile BASELINE_DIR
bash scripts/knife15-market-continuity.sh verify-profile PROFILE_FILE
```

Write the profile atomically with mode `0600`, record the source commit and
baseline SHA-256 manifest, and never overwrite an existing profile. Rates use
the already-established bounded baseline policy and are materialized once.
Every later trial reads the same absolute regular file and verifies its hash.

**Step 4: Run GREEN**

```bash
bash -n scripts/knife15-market-continuity.sh
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: both pass.

**Step 5: Commit**

```bash
git add scripts/knife15-market-continuity.sh scripts/knife15-market-iperf-summary.py
git commit -m "feat(knife15): freeze market calibration load profile"
git push origin codex/knife14d-downlink-reap-open
```

### Task 3: Selected-client and route preflight

**Files:**

- Modify: `scripts/knife15-market-continuity.sh`
- Test: `bash scripts/knife15-market-continuity.sh --self-test`

**Step 1: Add RED route fixtures**

The self-test must replay text fixtures for these exact cases:

1. Target route uses the expected VPN interface and Exit route uses the
   recorded physical interface — PASS.
2. Target and Exit both use the VPN interface — reject as a tunnel loop.
3. Target stays physical — reject as bypass.
4. a second utun owns the default route — reject as ambiguous client.
5. client label or profile hash differs from state — reject.

**Step 2: Run RED**

```bash
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: route-contract assertion fails.

**Step 3: Implement read-only client admission**

Support:

```bash
CLIENT_LABEL=mihomo-tuic \
EXPECTED_VPN_IF=utun9 \
PHYSICAL_IF=en0 \
EXPECTED_EXIT_IPV4=43.153.32.33 \
PROFILE_FILE=/absolute/profile \
bash scripts/knife15-market-continuity.sh preflight
```

Record, sanitize, and validate `route -n get` for Target, Exit, DNS target,
and default IPv4; `scutil --proxy`; active network service; operator-supplied
client label/version and binary identity when available; public egress; git
commit/status; iperf reachability; and clock. The script observes
`EXPECTED_VPN_IF` but never creates or removes it. It must not read or hash the
third-party client configuration. Credentials and environment values whose
names contain `PASSWORD`, `UUID`, `TOKEN`, `SECRET`, or `KEY` must be excluded
from evidence.

**Step 4: Run GREEN**

```bash
bash -n scripts/knife15-market-continuity.sh
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: pass.

**Step 5: Commit**

```bash
git add scripts/knife15-market-continuity.sh
git commit -m "feat(knife15): gate external-client calibration routes"
git push origin codex/knife14d-downlink-reap-open
```

### Task 4: Six-cycle workload that records and continues quality events

**Files:**

- Modify: `scripts/knife15-market-continuity.sh`
- Modify: `scripts/knife15-market-iperf-summary.py`
- Test: `bash scripts/knife15-market-continuity.sh --self-test`

**Step 1: Add RED workload replay**

Use a fake iperf executable in the self-test. It returns, in order, one clean
TCP result, one TCP result with two consecutive receiver-zero intervals, one
UDP result above 3% loss, and the remaining clean results. Assert that the
runner:

- executes every scheduled phase;
- records both quality events;
- exits success-with-events rather than aborting;
- aborts on malformed JSON or command timeout;
- excludes the final partial interval.

**Step 2: Run RED**

```bash
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: workload-continuation assertion fails.

**Step 3: Implement bounded phase execution**

Support:

```bash
CLIENT_LABEL=mihomo-tuic \
EXPECTED_VPN_IF=utun9 \
PHYSICAL_IF=en0 \
EXPECTED_EXIT_IPV4=43.153.32.33 \
PROFILE_FILE=/absolute/profile \
EXIT_SSH_HOST=ubuntu@43.153.32.33 \
EXIT_SSH_KEY="$HOME/.ssh/vpn" \
bash scripts/knife15-market-continuity.sh run
```

Run six cycles with exact durations `300/300/180/10x6`. Give each iperf
command a hard timeout of `duration + 30` seconds. A valid low-quality result
is appended to `events.tsv` and the run proceeds. Invalid evidence sets
`INVALID`, triggers bounded finalization, and returns nonzero. Record monotonic
and wall-clock phase boundaries so Mac and Exit evidence can be aligned.

**Step 4: Run GREEN**

```bash
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: all schedule and stop-rule cases pass.

**Step 5: Commit**

```bash
git add scripts/knife15-market-continuity.sh scripts/knife15-market-iperf-summary.py
git commit -m "feat(knife15): run bounded market continuity cycles"
git push origin codex/knife14d-downlink-reap-open
```

### Task 5: Observer binding, signal finalization, and immutable bundle

**Files:**

- Modify: `scripts/knife15-market-continuity.sh`
- Reuse: `scripts/knife15-exit-target-observer.sh`
- Test: `bash scripts/knife15-market-continuity.sh --self-test`

**Step 1: Add RED lifecycle tests**

Mock the observer command and assert:

- pre-existing observer ownership rejects before traffic without stopping it;
- the runner starts one fresh observer after route preflight and binds its
  exact returned identity before traffic;
- normal completion freezes and bundles exactly once;
- `INT`, `TERM`, workload error, and quality-event completion each finalize;
- observer finalization failure makes evidence invalid;
- no path calls `kill`, `networksetup`, or client stop commands for the
  external client.

**Step 2: Run RED**

```bash
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: lifecycle assertion fails.

**Step 3: Implement exact observer ownership**

After client/route/profile preflight and before traffic, require that no
observer state already exists, start one v2 observer through the existing
script, and bind the exact returned run identity. Its Exit, Target, TUIC port,
iperf port, start age, components, and capture-drop state must match the trial.
Install one idempotent trap that freezes and bundles only this runner-started
observer. Save its reported remote SHA-256 in the Mac manifest. Never reuse an
observer between clients or trials, and never stop ambiguous pre-existing
observer ownership.

Bundle the Mac run to:

```text
/tmp/mini_vpn_knife15_market_<client>_<UTC>.tar.gz
```

Write a sibling `.sha256`. The bundle includes raw JSON, normalized summaries,
events, routes, profile+hash, source identity, observer identity/hash, and a
machine-readable final status of `PASS_NO_EVENTS`, `PASS_WITH_EVENTS`, or
`INVALID`.

**Step 4: Run GREEN**

```bash
bash -n scripts/knife15-market-continuity.sh
bash scripts/knife15-market-continuity.sh --self-test
```

Expected: pass and no leaked mock observer ownership.

**Step 5: Commit**

```bash
git add scripts/knife15-market-continuity.sh
git commit -m "feat(knife15): bind market trials to exit evidence"
git push origin codex/knife14d-downlink-reap-open
```

### Task 6: Decision summary and operator runbook

**Files:**

- Modify: `scripts/knife15-market-continuity.sh`
- Create: `docs/tech/2026-08-14-knife15-m2-market-continuity-calibration-runbook.md`
- Modify: `docs/tech/2026-07-30-knife15-m2-macos-hitl-runbook.md`
- Modify: `HANDOFF.md`
- Modify: `TODO.md`
- Modify: `AGENTS.md`
- Modify: `.learnings/LEARNINGS.md`
- Modify: `.learnings/ERRORS.md`

**Step 1: Add RED decision-summary tests**

Replay normalized trial summaries and assert exact outcomes for:

- mature client has receiver-zero events — `CALIBRATE_PRODUCT_SLI`;
- mature passes twice and mini_vpn fails twice — `MINI_VPN_DIFFERENTIAL`;
- all matched trials pass — `EXTEND_MATCHED_DURATION`;
- invalid evidence — `NO_DECISION`.

The classifier reports evidence; it must not select a custom protocol.

**Step 2: Implement summary and runbook**

Add:

```bash
bash scripts/knife15-market-continuity.sh summarize RUN_DIR...
```

The runbook must keep credentials as placeholders and separate these operator
boundaries:

1. VPN off: direct baseline and immutable profile;
2. mature TUIC client on: verify same `.33` server, route, and egress;
3. run C0; the harness starts/binds a fresh observer and collects both bundles;
4. verify both final bundle checksums;
5. stop the external client manually only after finalization;
6. run C1 only if the spec's decision tree requires it.

Keep the former formal M2 runbook blocked and link to the calibration runbook.

**Step 3: Run local gates**

```bash
bash -n scripts/knife15-market-continuity.sh
python3 -m py_compile scripts/knife15-market-iperf-summary.py
python3 scripts/knife15-market-iperf-summary.py --self-test
bash scripts/knife15-market-continuity.sh --self-test
git diff --check
git status --short
```

Expected: all pass; only intended files are modified.

**Step 4: Review**

Use the `code-review` skill. Review invalid-evidence fail-closed behavior,
external-client non-ownership, route/observer exactness, result parsing,
timeouts/signals, secret leakage, evidence boundedness, and whether any path
can accidentally run formal M2. Resolve every P0/P1 before HITL.

**Step 5: Commit and push**

```bash
git add AGENTS.md HANDOFF.md TODO.md .learnings/LEARNINGS.md .learnings/ERRORS.md \
  scripts/knife15-market-continuity.sh scripts/knife15-market-iperf-summary.py \
  scripts/fixtures/knife15-market docs/tech
git commit -m "docs(knife15): prepare market continuity calibration"
git push origin codex/knife14d-downlink-reap-open
```

### Task 7: Execute the C0 HITL discriminator

**Files:**

- Create after evidence: `docs/tech/2026-08-*-knife15-m2-market-continuity-calibration-results.md`
- Modify after evidence: `HANDOFF.md`
- Modify after evidence: `TODO.md`
- Modify after evidence: `AGENTS.md`
- Modify after evidence: `.learnings/LEARNINGS.md`
- Modify after failure-changing evidence: `.learnings/ERRORS.md`

**Step 1: Verify source and self-tests on the HK Mac**

Follow only the new calibration runbook. Verify a clean tracked worktree, the
reviewed source floor, both self-tests, physical interface, target/exit health,
and no other VPN.

**Step 2: Create one profile with VPN off**

Run one direct baseline and materialize its immutable profile. Record its
absolute path and SHA-256. Reuse it unchanged for every C0/C1 client trial.

**Step 3: Run mature TUIC C0**

Connect the mature client to the exact `.33` TUIC server and verify routes and
public egress. Let the calibration harness start and bind a fresh `.33`
observer, then execute six cycles. Preserve both Mac and Exit bundles even when
quality events occur.

**Step 4: Classify before spending more time**

Apply the specification's C0 decision tree. If the mature client records a
valid receiver-zero event, do not run C1 merely to accumulate hours. If it has
zero events, execute only the matched C1 trials required for a decisive result.

**Step 5: Record and commit the accepted result**

Write exact artifact paths/checksums, client version/binary identity, profile
hash/rates, route/observer evidence, every quality event, and the decision.
Never read or hash the third-party config. Run docs/diff/secret review, update
project memory, commit, and push.
