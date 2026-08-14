# Knife15 M2 Strict Resource Binding Local Results

Date: 2026-08-14

Status: **PASS — LOCAL IMPLEMENTATION AND REVIEW COMPLETE; NO MAC/VPS RUN
AUTHORIZED YET**

## Outcome

Strict `m2-qualification` and formal `m2` now fail closed before workload
traffic unless they can import and revalidate one eligible immutable resource
preflight. The run owns the exact candidate ID, canonical profile SHA-256,
preflight result SHA-256, Exit IP/TUIC port, Target/iperf port, physical Mac
interface, source, binary, direct evidence, observer, server hashes, remote
health evidence, and sealed preflight archive.

Implementation commits:

- `ef5a433` — bind strict M2 to resource evidence;
- `18c0e14` — require `ef5a433` or a descendant at strict source admission.

No Rust production code, QUIC/TUIC behavior, workload duration/rate, strict
`receiver_zero == 0` requirement, UDP `3%` limit, TCP gap limit, D16/Endpoint
invariant, or cleanup contract changed.

## Closed contracts

- Missing, path-unsafe, symlinked, changed, ineligible, or internally
  inconsistent resource evidence is rejected before full-tunnel activation or
  workload registration.
- `.33` remains the historical failed reference and cannot be admitted as a
  new candidate because equal candidate identity or Exit IPv4 is ineligible.
- The preflight directory, its external tar SHA-256, the complete tar member
  set, every archived member, the copied run directory, its internal
  `SHA256SUMS`, the stage workload profile, and root-owned state must agree.
- Duplicate JSON keys are rejected in both the profile helper and runner
  lookup path.
- The paired Exit observer is admitted only after the resource candidate is
  bound, and its SSH IP, TUIC port, and Target must equal the recorded run and
  candidate.
- Status and final summary publish resource admission, candidate ID, and
  profile SHA-256; the bundle contains the copied evidence and archive.

## Review repairs before commit

Concentrated review found and repaired three locally preventable risks:

1. Linux `/proc/meminfo` keys retained a colon, so Mac validation would reject
   a healthy real preflight. The remote probe now emits canonical
   `memtotal=`/`memavailable=` fields.
2. A copied evidence directory could previously be changed after import while
   the independently valid tar archive remained unchanged. Every decisive
   binding now compares the directory and every tar member again.
3. Directory evidence was not independently tied to root-owned runner state.
   Stage, candidate, profile, and result hashes are now persisted and checked
   before route mutation and at final strict acceptance.

No unresolved P0/P1 remains in this stage.

## Local gates

PASS:

```text
bash -n scripts/knife15-macos-soak.sh
bash -n scripts/knife15-m2-resource-preflight.sh
/usr/bin/python3 -I scripts/knife15-m2-resource-profile.py --self-test
bash scripts/knife15-m2-resource-preflight.sh --self-test
bash scripts/knife15-macos-soak.sh --self-test
bash scripts/knife15-exit-target-observer.sh --self-test
git diff --check
frozen-value diff scan
credential-pattern scan (only documented placeholder/runtime variable names)
```

The complete runner self-test exits zero with
`passfailpassknife15 macOS runner self-test passed`. Its earlier
`ERROR: command exceeded hard timeout of 1s` line is the expected timeout
fixture.

## Next gate

Task 4 is the strict-attempt ledger. Do not provision or run a Mac candidate
until the ledger, operator runbook, full local gates, and final code review are
complete. Formal M2 and M3 remain blocked.
