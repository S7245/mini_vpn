# Knife15 M2 24-Hour Real-Client Soak Implementation Plan

Date: 2026-07-30

Status: **LOCAL PASS — implementation, gates, review, and runbook complete;
user-run HK macOS M2 pending**

Source of truth:
`docs/tech/2026-07-30-knife15-m2-24h-real-client-soak-architecture-spec.md`.

Goal: extend the accepted Knife15 runner with one fail-closed `m2` action that
owns a controlled IPv4 full tunnel, system DNS, a 24-hour mixed/real-client
schedule, resource/lifecycle checkpoints, leak detection, cleanup, summary,
and immutable acceptance evidence.

No Rust data-plane or frozen value changes are in scope.

## File Map

- Modify `scripts/knife15-macos-soak.sh`: public M2 contract, route/DNS owner,
  profile/schedule, real-client probes, checkpoints, summary, cleanup, and
  dispatcher.
- Keep `scripts/knife15-macos-soak-self-test.sh` as the external public
  self-test wrapper.
- Create
  `docs/tech/2026-07-30-knife15-m2-24h-real-client-soak-local-results.md`
  after all local gates pass.
- Create
  `docs/tech/2026-07-30-knife15-m2-macos-hitl-runbook.md`
  with the exact user sequence only after the reviewed command is stable.
- Update project memory and `.learnings/` only after local acceptance.

## TDD Slices

### Task 1: Lock the public M2 contract

- [x] RED help/dispatcher tests for `m2`, `M2_BASELINE_DIR`, and
  `M2_DIRECT_DIR`.
- [x] GREEN only the public action, variables, and workload identity.
- [x] Preserve exact M0/M1/M1-diagnostic identity behavior.

### Task 2: Add route, DNS, and IPv6 pure parsers

- [x] RED fixtures for macOS network-service order, DNS empty/nonempty
  snapshots, route interface/gateway, fake IPv4 range, and routable IPv6.
- [x] GREEN parser helpers without real route or DNS mutation.
- [x] Reject malformed, ambiguous, disabled, multiline-injected, or missing
  service evidence.

### Task 3: Add idempotent M2 full-tunnel ownership

- [x] RED fake-command fixtures for the exact mutation order:
  Exit `/32`, two IPv4 split defaults, fake-IP route, DNS set, cache flush.
- [x] RED partial-failure fixtures after each mutation.
- [x] RED cleanup fixtures for success, repeated cleanup, route mismatch, DNS
  mismatch, and an unrelated route/DNS owner.
- [x] GREEN `activate_m2_full_tunnel`,
  `m2_full_tunnel_is_active`, and
  `deactivate_m2_full_tunnel`.
- [x] Wire only the shared watchdog/stop cleanup seam after pure fixtures pass.

### Task 4: Add the immutable M2 profile

- [x] RED exact `86,400s` duration arithmetic, segment order, rates, result
  counts, checkpoint count, and `4GiB` start-free requirement.
- [x] GREEN `write_m2_profile` and `validate_m2_formal_config`.
- [x] Preserve M1 rate math and the `200,000,000 bit/s` workload cap.

### Task 5: Add a real-client probe seam

- [x] RED fake `curl` fixtures for Exit IP, fake remote IP, HTTPS status/bytes,
  route/DNS state, command timeout, malformed metadata, wrong egress, real-IP
  DNS leak, and external failure.
- [x] GREEN one sanitized preflight/cycle probe; discard response bodies.
- [x] Prove the probe uses the system resolver and never `--resolve` or proxy
  environment.

### Task 6: Implement the exact M2 schedule

- [x] RED a compressed six-active/five-idle/final fixture with unchanged
  steady/quiet/churn ordering.
- [x] GREEN a separate `run_m2_schedule`; reuse stage-aware phase helpers.
- [x] Add the real-client hook only after each complete M2 cycle.
- [x] Require exact formal counts:
  `93` cycles/DNS/probes, `934` TCP, `95` UDP, `1029` results.

### Task 7: Add six lifecycle/resource checkpoints

- [x] RED checkpoint fixtures for exact labels, fresh Endpoint/data-plane
  samples, resources, zero active relay/fake-IP ownership, a stable
  at-most-two-entry fake-IP cache, zero DNS drop, and conservation.
- [x] RED one-at-a-time negative fixtures for each resource and ownership
  limit.
- [x] GREEN `capture_m2_checkpoint`, envelope, and SLO.

### Task 8: Add M2 summary and two-phase acceptance

- [x] RED a complete fixture requiring route/DNS, real-client, timeline,
  result, checkpoint, sample, ownership, log/disk, rebind, and SLO fields.
- [x] RED missing/malformed/mutated evidence and cleanup mismatch fixtures.
- [x] GREEN `m2_slo_evidence=PASS` before stop and
  `formal_m2_acceptance=PENDING_CLEANUP`.
- [x] GREEN final `formal_m2_acceptance=PASS` only after cleanup evidence.

### Task 9: Add the formal M2 action

- [x] RED prerequisites: accepted ancestor, fresh M2 baseline/direct, DNS,
  controls, pool idle, tools, disk, IPv6, no previous stage evidence.
- [x] GREEN the sequence:
  prepare -> activate -> preflight probe -> register -> schedule -> summarize.
- [x] Any failure leaves the TUN/evidence available for documented
  `status/snapshot/stop`; partial activation is still safely cleanable.

### Task 10: Preserve previous public behavior

- [x] Re-run every existing M0/M1/diagnostic, observer, route, signal,
  one-shot bundle, secret, and cleanup fixture.
- [x] Prove `start` remains target-only and changes neither default route nor
  system DNS.
- [x] Prove no M2 state changes M1 summary semantics.

### Task 11: Full local gates and code review

- [x] Runner self-test and external wrapper.
- [x] Knife14 low-RTT, US client suite, and sing-box control self-tests.
- [x] Bash syntax.
- [x] Root all-targets+harness, main, integration, release, and Clippy.
- [x] Vendored Quinn/proto gates required by current project memory.
- [x] `cargo fmt`, `git diff --check`, changed-content secret scan.
- [x] Review route/DNS ownership, PID/child identity, signals, shell quoting,
  macOS Bash 3.2/BSD compatibility, count math, disk bounds, cleanup, result
  direction, cumulative counters, and fail-closed behavior.
- [x] Repair every P0/P1 with a focused RED/GREEN and rerun affected gates.

### Task 12: Results, runbook, memory, commit, and push

- [x] Record exact RED/GREEN and gate evidence in the M2 local-results doc.
- [x] Publish a one-shot macOS HITL runbook; do not reuse old M0 commands.
- [x] Update `AGENTS.md`, `HANDOFF.md`, `TODO.md`,
  `.learnings/LEARNINGS.md`, and `.learnings/ERRORS.md` if a failure changes
  future behavior.
- [x] Secret scan staged content.
- [x] Commit one coherent conventional commit and push the current branch.

### Task 13: Repair real-macOS IPv6 route-status observation

- [x] RED the observed status-zero `not in table` output; retain unknown and
  physical fail-closed fixtures.
- [x] GREEN one shared four-state classifier for the public pre-baseline check,
  formal M2, health checks, and real-client evidence.
- [x] Add read-only `m2-ipv6-check` using the exact formal probe.
- [x] Persist formal route status/text/class/interface before rejection and
  publish the decisive fields in status/summary.
- [x] Update the runbook so no baseline begins without an exact check PASS.
- [x] Re-run internal/external shell gates, Bash 3.2 syntax, the original local
  macOS reproduction, review, diff, link, and secret gates.

## User Execution Stop Position

Only after Task 12 passes may the user run:

```text
fresh terminal
disable Clash-TUN and every other VPN/TUN
build -> self-test -> preflight
fresh baseline -> fresh direct discriminator
start -> smoke -> m2 -> status -> stop
```

The reviewed runbook will provide exact commands and expected durations. No
real macOS TUN is part of local implementation.

## Repair Rule

Expected RED enters its minimal GREEN. Unexpected local failure is first
classified against the architecture spec; the user's standing instruction
authorizes the safest in-scope repair without repeated confirmation. Frozen
values, scope expansion, destructive external changes, or new authority still
require a stop.
