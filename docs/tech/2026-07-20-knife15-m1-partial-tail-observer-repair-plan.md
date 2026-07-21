# Knife15 M1 Partial-Tail Observer Repair Plan

Date: 2026-07-20

Status: **LOCAL PASS — diagnosis, focused TDD, artifact replay, and review
complete; no real TUN repeat**

## Evidence And Root Cause

The user-run archive
`/tmp/mini_vpn_knife15_macos_20260720_090636.tar.gz`, SHA-256
`5fd183b10ce7c9a0333795d86127005322bac752c1f303f43f7b160d62806578`,
matches the supplied checksum and exact source `dc8cfb1`.

M1 completed five full mixed cycles. Cycle 6 forward TCP then transferred
`292,945,920B` to the Target over `300.165s`, but the runner reported
`receiver_zero_interval`. All 300 complete Target receiver intervals were
positive. The only zero row covered `300.001041-300.164959s`, a `0.163918s`
iperf command-tail observation after the formal 300-second window.

The runner's direct discriminator already defines a complete receiver interval
as at least `0.5s`, but the shared baseline/phase/summary validators applied the
positive-rate requirement to every iperf interval including a shorter tail.
This mismatch caused an evidence-classifier false failure. It is not a
mini_vpn data-plane failure and not an operator/Clash error.

## Goal

Make every direction-aware continuity decision use one conservative command-
tail classifier:

```text
proven partial tail:
  numeric command duration and interval timing
  final interval entry only
  start >= duration - 0.5 seconds
  duration <= end <= duration + 0.5 seconds
  0 <= end - start < 0.5 seconds
```

A zero proven-partial tail is excluded from the one-second continuity SLI. A
zero complete interval, a zero nonterminal short interval, a zero interval
without numeric timing proof, or any otherwise malformed result still fails
closed.

## Frozen Boundary And Non-Goals

Do not change H10d16, EndpointWindowV1, MTU, UDP payload, pool, QUIC windows,
chunk size, Cubic, GSO, D16 capacities, self-wake, M1 duration/rates, direct
freshness, resource SLOs, recovery rules, or cleanup.

This repair does not accept the interrupted M1 as an eight-hour PASS. It only
reclassifies its completed 300-second phase correctly and requires a fresh
full M1 after local gates and review pass.

## TDD Slices

1. RED: append a real-shaped zero `0.164s` Target receiver tail to an otherwise
   valid forward TCP result. Phase validation and summary continuity must PASS.
2. GREEN: exclude only timing-proven intervals shorter than `0.5s` from
   receiver/sender zero counts and positive-window checks.
3. RED/GREEN guard: change the interval to a zero `1.0s` window; validation
   must return `receiver_zero_interval` and summary must count it.
4. Fail-closed guard: a zero interval with missing/non-numeric timing must
   remain a receiver failure.
5. Apply the same semantics to baseline summaries/validation, direct manifest
   counts, phase validation/failure reasons, and final result envelopes.

## Acceptance

- the archived cycle 6 result replays as `ok` with zero complete receiver
  stalls and one diagnostic partial zero;
- every existing real complete-zero fixture still fails;
- Knife15 internal/external shell tests and Bash syntax pass;
- Knife14 shell controls, Rust regressions, release, Clippy, fmt, diff, and
  secret checks pass;
- code review has no unresolved P0/P1;
- no real TUN or VPS run occurs in this repair stage.

If the focused repair or regression gate fails unexpectedly, stop, analyze the
failed invariant, and revise this plan before any further modification.

## Completed Result

All slices above are GREEN. Review additionally found and repaired the missing
final-entry requirement before acceptance. The archived cycle 6 result now
replays as `ok` with `300` complete positive receiver intervals and one proven
`0.163918s` partial zero tail. Complete, nonterminal, and timing-unproven zero
fixtures still fail closed. Full local gates pass with no unresolved P0/P1.
Result:
`docs/tech/2026-07-20-knife15-m1-partial-tail-observer-repair-results.md`.
