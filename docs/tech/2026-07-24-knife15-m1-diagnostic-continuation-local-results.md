# Knife15 M1 Diagnostic Continuation Local Results

Date: 2026-07-24

Status: **Local implementation accepted — one fresh user-run HK macOS
`m1-diagnostic` pending**

## Outcome

The macOS HITL runner now has a separate public `m1-diagnostic` action. It
executes the unchanged formal M1 `28,800s` schedule and continues after valid
data-quality observations so one eight-hour window can produce a complete
longitudinal artifact. The implementation commit is `b675540`.

Formal `m1` remains fail-fast and is the only action that can produce formal
M1 acceptance. No Rust data-plane code or frozen Knife14/Knife15 value
changed.

## Continuable Evidence

The diagnostic action records and continues:

- complete direction-aware Target receiver-zero intervals;
- valid reverse-UDP loss above `3.0%`;
- a final aggregate TCP sender/receiver gap above `16,777,216B`.

Every observation is written to `m1-diagnostic-violations.tsv` with cycle,
phase, value, detail, and run-relative source evidence. Receiver-zero detail
includes all relative iperf interval ranges.

The final summary replays every row against its original JSON. It also scans
all results to prove that no receiver-zero, high-loss UDP, or aggregate-gap
observation is missing from the ledger.

## Fail-Closed Boundary

The continuation policy is default-off and enabled only for
`m1-diagnostic`. A receiver-zero result must pass a second complete validator
that relaxes only the positive receiver-interval predicate. Missing sender or
receiver evidence, invalid fields, wrong protocol/direction, malformed JSON,
command failure, hard timeout, DNS failure, TUN/route/watchdog health failure,
checkpoint failure, incomplete samples, Endpoint/D16/pump/TUN safety signals,
and final ownership/resource/recovery mismatches remain failures.

The review specifically found and repaired a potential fail-open case where
`receiver_zero_interval` could otherwise have hidden an unrelated malformed
sender field.

## Status And Acceptance

Diagnostic terminal status is:

```text
diagnostic_complete_clean
diagnostic_complete_with_violations
failed
interrupted
```

The summary publishes:

```text
m1_mode: diagnostic
formal_m1_acceptance: NOT_APPLICABLE
m1_result_integrity_evidence
m1_diagnostic_violation_count
m1_diagnostic_safety_evidence
```

A diagnostic completion never evaluates or passes `m1_slo_evidence`. Its
process exits successfully only when the complete timeline and non-data-
quality safety evidence pass; the terminal output explicitly says it is not
formal acceptance.

## TDD Coverage

Focused shell fixtures prove:

- a complete receiver-zero interval records and continues;
- missing receiver evidence still fails;
- receiver-zero plus malformed sender evidence still fails;
- UDP `3.000001%` records and continues while exactly `3.0%` does not;
- a compressed diagnostic schedule reaches final drain with multiple
  violations;
- an iperf command failure stops that same schedule immediately;
- full `30`-cycle / `332`-result diagnostic evidence passes safety while
  formal acceptance stays `NOT_APPLICABLE`;
- deleting a required violation row fails source coverage;
- pump/resource safety signals fail diagnostic completion;
- an aggregate TCP gap is recorded without being mistaken for a safety PASS
  or formal M1 acceptance;
- the existing formal M1 fixture still produces
  `formal_m1_acceptance: PASS`.

## Local Gates

```text
Knife15 runner internal self-test                 PASS
Knife15 wrapper self-test                         PASS
Knife14 sing-box control self-test                PASS
shell syntax                                      PASS
root all-targets + harness                        666 passed / 3 ignored
main                                               2 passed
integration harness                               10 passed / 4 ignored
cargo build --release                             PASS
cargo clippy --all-targets --features harness     PASS (existing warnings)
cargo fmt --all -- --check                        PASS
git diff --check                                  PASS
changed-content secret scan                       PASS
code review                                       no unresolved P0/P1
```

The Knife15 internal self-test still intentionally prints the one-second
timeout error from its negative deadline fixture; the final exit status and
PASS line are authoritative.

## Next Action

Take one fresh user-operated HK macOS run from the pushed source:

```text
baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic
-> status -> stop
```

Use fresh `M1_BASELINE_DIR` and `M1_DIRECT_DIR`, rebuild release, and keep
Clash-TUN and every other VPN/TUN disabled from baseline through `stop`.
Preserve `status/snapshot/stop` on a safety failure.

The resulting bundle is for longitudinal attribution and optimization
planning. It cannot unblock M2/M3. Formal M1 still requires a separate fresh
`m1` run whose TCP/UDP SLOs all pass.
