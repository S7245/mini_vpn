# Knife15 M2 Tiered Continuity And Resource Strategy Implementation Plan

Status: **TASKS 1–3 COMPLETE THROUGH `18c0e14`; TASK 4 STRICT ATTEMPT
LEDGER IS NEXT; FORMAL M2 AND M3 REMAIN BLOCKED**

> **For agentic workers:** use `diagnose` and `tdd` for each behavior change;
> use `code-review` before requesting a long macOS run.

**Goal:** Execute at most two materially different strict resource candidates,
then—only if both fail—measure an exact low-frequency one-second interruption
budget without weakening the existing formal M2 action.

**Architecture:** Keep production Rust and the strict `m2` contract frozen.
Add a resource identity/admission layer and an immutable attempt ledger around
the existing runner. Add the separately named Tier-B reducer and epoch action
only after the ledger proves Tier A exhausted. Reuse the already-reviewed
iperf interval reducer where its semantics match; remove all dependency on an
external mature VPN client.

**Tech Stack:** POSIX-oriented Bash on macOS, Python 3 standard library,
iperf3 JSON, existing Knife15 macOS runner and Exit observer, SSH, route and
system evidence, SHA-256 manifests, tar bundles, Rust workspace gates.

---

## Implementation Rules

- [ ] Do not change Rust production code or any frozen data-plane constant in
  this plan.
- [ ] Do not modify the strict `m2` receiver-zero requirement.
- [ ] Do not run or extend the market-client C0 harness.
- [ ] Every new parser/decision branch starts with a deterministic RED.
- [ ] Expected RED may proceed directly to the smallest GREEN.
- [ ] A non-expected regression must be diagnosed and repaired according to
  the accepted plan; do not compensate by tuning a threshold.
- [ ] Commit and push each coherent completed task.
- [ ] Do not request a long Mac run until local gates and code review pass.

### Task 1: Supersede the market-comparison route

**Files:**

- Modify: `docs/tech/2026-08-14-knife15-m2-market-continuity-calibration-spec.md`
- Modify: `docs/tech/2026-08-14-knife15-m2-market-continuity-calibration-implementation-plan.md`
- Modify: `docs/tech/2026-08-14-knife15-m2-market-continuity-calibration-runbook.md`
- Modify: `docs/tech/2026-08-14-knife15-m2-market-continuity-calibration-local-results.md`
- Modify: `docs/tech/2026-08-14-knife15-m2-ack-progress-native-loss-recovery-formal-failure-results.md`

**Steps:**

1. Mark the old comparator decision `SUPERSEDED; DO NOT RUN` without deleting
   its implementation or historical review evidence.
2. Link this specification and state that the existing interval reducer may
   be reused only as a local evidence component.
3. Assert by repository search that no current handoff or TODO asks the user
   to start a mature TUIC client.
4. Commit and push the documentation-only decision.

### Task 2: Resource-candidate manifest and admission RED/GREEN

**Files:**

- Create: `scripts/knife15-m2-resource-profile.py`
- Create: `scripts/knife15-m2-resource-preflight.sh`
- Create: `scripts/fixtures/knife15-m2-resource/reference-33.json`
- Create: `scripts/fixtures/knife15-m2-resource/distinct-provider.json`
- Create: `scripts/fixtures/knife15-m2-resource/equivalent-resize.json`

**Steps:**

1. Add failing self-tests for a complete immutable profile, a distinct
   provider-and-ASN candidate, and an ineligible same-route resize.
2. Implement canonical JSON validation and SHA-256 output. Require candidate
   ID, provider/resource ID, region, public IPv4, ASN, route class, TUIC port,
   Target, server hashes, Mac route fingerprints, source, binary, profile, and
   observer hashes.
3. Make eligibility compare the candidate with the immutable `.33` reference.
   Accept a distinct provider and ASN, or an independently contracted route;
   reject a nominal resize without exact prior saturation evidence.
4. Keep read-only resource admission isolated from the root-owned TUN runner:
   add `knife15-m2-resource-preflight.sh`. It validates the exact profile,
   source, binary, direct profile, local route/interface, remote service,
   server hashes, and headroom evidence without mutating routes or starting
   TUN.
5. Run both helper self-tests and the unchanged complete runner self-test.
6. Commit and push.

### Task 3: Bind strict runs to one immutable resource profile — COMPLETE

**Files:**

- Modify: `scripts/knife15-macos-soak.sh`
- Modify: `scripts/fixtures/knife15-macos-soak/`

**Steps:**

1. Add REDs showing `m2-qualification` and `m2` reject a missing, changed, or
   ineligible resource-profile hash before workload traffic.
2. Persist candidate ID and profile hash in root-owned runner state, the run
   manifest, status, snapshot, and final bundle.
3. Bind the Exit observer to the profile's exact Exit IP/TUIC port and `.77`
   Target rather than assuming `.33`.
4. Preserve the existing `.33` history parser but forbid `.33` as a new
   candidate.
5. Prove strict `receiver_zero == 0`, all frozen workload constants, and the
   cleanup contract are unchanged.
6. Run focused and complete runner/observer self-tests.
7. Commit and push.

Completed by `ef5a433` plus source-floor closure `18c0e14`. Result:
`docs/tech/2026-08-14-knife15-m2-strict-resource-binding-local-results.md`.

### Task 4: Strict-attempt ledger RED/GREEN

**Files:**

- Create: `scripts/knife15-m2-continuity-ledger.py`
- Create: `scripts/fixtures/knife15-m2-ledger/`

**Steps:**

1. Add RED fixtures for `.33` historical failure, invalid environmental run,
   qualification failure, one formal pass, two consecutive formal passes, and
   two rejected candidates.
2. Validate bundle SHA-256, source/binary/profile/resource identity, role,
   strict SLI, observer match, and cleanup before admitting a record.
3. Ensure invalid runs consume no candidate slot and supply no pass; genuine
   quality failures reject the exact candidate; a new candidate configuration
   requires a new eligible resource identity.
4. Emit only `TIER_A_PENDING`, `TIER_A_ACCEPTED`, or `TIER_A_EXHAUSTED`, with
   exact supporting bundle hashes.
5. Prove Tier A acceptance requires two consecutive valid strict formal
   passes on one immutable candidate.
6. Run self-tests, syntax checks, and secret scan.
7. Commit and push.

### Task 5: Strict-resource operator runbook and local review gate

**Files:**

- Create: `docs/tech/2026-08-14-knife15-m2-strict-resource-macos-runbook.md`
- Create: `docs/tech/2026-08-14-knife15-m2-strict-resource-local-results.md`

**Steps:**

1. Document resource provisioning, server config/hash capture, profile
   creation, read-only preflight, direct baseline, qualification, observer,
   strict formal runs, status/snapshot/stop, and bundle sync.
2. State explicitly that `.111` is not accepted merely because it exists; it
   must first pass the new eligibility and capacity admission.
3. Run shell syntax, helper and runner self-tests, root tests, release build,
   established Clippy, fmt/diff, provenance, and secret checks.
4. Perform `code-review` across resource identity, remote ownership, cleanup,
   false-pass, and false-rejection paths. Resolve all P0/P1 findings.
5. Record the exact reviewed commit and commands in local results.
6. Commit and push; only then issue the Mac/VPS operations list.

### Task 6: Execute at most two strict candidates

**Evidence:**

- Create one immutable resource profile and evidence set per candidate.
- Produce paired Mac and Exit bundles for every admitted qualification or
  formal run.

**Steps:**

1. Provision the first candidate on a materially different path and pass
   `resource-preflight`.
2. Run one strict qualification. On a genuine failure, seal it and reject the
   candidate; on invalid infrastructure evidence, repair the environment and
   rerun without consuming the slot.
3. After a qualification pass, run two consecutive strict formal M2s. Stop at
   the first genuine failure; accept Tier A only if both pass and clean up.
4. If candidate 1 is rejected, repeat once for candidate 2.
5. Run the ledger after each bundle. Do not add a third candidate or tune the
   failed configuration.
6. If the ledger emits `TIER_A_ACCEPTED`, write the acceptance result and
   reopen M3. If it emits `TIER_A_EXHAUSTED`, proceed to Task 7.

### Task 7: Tier-B episode reducer RED/GREEN, only after exhaustion

**Precondition:** the strict ledger must emit `TIER_A_EXHAUSTED` from two
eligible candidate histories.

**Files:**

- Create: `scripts/knife15-m2-frequency-summary.py`
- Create: `scripts/fixtures/knife15-m2-frequency/`
- Reuse: `scripts/knife15-market-iperf-summary.py`

**Steps:**

1. Add RED fixtures for zero episodes, three isolated episodes/day, two
   consecutive zero intervals, two episodes/six hours, four episodes/day, a
   partial final interval, and a missing epoch.
2. Reuse the reviewed complete-interval parsing semantics without invoking
   the old external-client harness.
3. Compute maximal episodes and exact rolling six-/24-hour windows from UTC
   and monotonic timestamps.
4. Fail closed on a missing interval, clock reversal, profile/hash mismatch,
   invalid epoch, or evidence gap. Never bridge an unknown period.
5. Require all four exact Tier-B inequalities from the specification.
6. Run self-tests and property-style boundary cases at exactly six and 24
   hours.
7. Commit and push.

### Task 8: Separate Tier-B epoch action and immutable ledger

**Files:**

- Modify: `scripts/knife15-macos-soak.sh`
- Modify: `scripts/knife15-m2-continuity-ledger.py`
- Modify: `scripts/fixtures/knife15-macos-soak/`

**Steps:**

1. Add REDs proving formal `m2` remains strict and cannot consume Tier-B
   flags or summaries.
2. Add a separately named `m2-frequency` action admitted only by a valid
   `TIER_A_EXHAUSTED` ledger and a new reviewed source floor.
3. Seal a six-hour epoch with exact source/binary/profile/resource/observer
   identity and complete phase results; incomplete epochs are invalid.
4. Allow valid epochs to accumulate across invalid infrastructure events, but
   require twelve valid epochs and at least one uninterrupted four-epoch
   24-hour process/TUN lifetime.
5. Preserve the existing UDP, gap, DNS, real-client, ownership, resource,
   observer, and cleanup gates.
6. Add signal and cleanup RED/GREEN cases so a failed epoch leaves actionable
   evidence without manufacturing a pass.
7. Run complete runner and observer self-tests.
8. Commit and push.

### Task 9: Tier-B review, HITL run, and architecture stop

**Files:**

- Create: `docs/tech/2026-08-14-knife15-m2-frequency-macos-runbook.md`
- Create after evidence: `docs/tech/2026-08-14-knife15-m2-frequency-results.md`

**Steps:**

1. Complete the same local build, test, lint, provenance, secret, and
   `code-review` gates as Task 5.
2. Run twelve valid six-hour epochs, including one uninterrupted four-epoch
   lifetime, under one immutable profile.
3. Accept Tier B only if the reducer and ledger pass every rolling-window and
   existing safety gate and cleanup is complete.
4. If Tier B passes, document the accepted product SLI and reopen M3 without
   claiming Tier-A continuity.
5. If Tier B fails, stop standard-TUIC resource/parameter work and create the
   architecture spec for path-diverse or resumable upstream byte ownership.
