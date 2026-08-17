# Knife15 M2 Tier-B Frequency Controller Local Results

Date: 2026-08-17

Status: **PASS; FIRST FOUR-EPOCH RUN NOT STARTED**

## Scope

Task 9 adds only the detached macOS execution owner and its runbook. It does
not change Rust production code, strict `m2`, the Tier-B reducer/ledger,
traffic shape, MTU, pool, QUIC windows, Cubic, Endpoint pacing, D16, GSO, or
any frozen acceptance value.

The controller owns one already-prepared `m2-frequency` action. It keeps the
same-TTY sudo ticket alive while the root workload runs, sends the requested
best-effort `执行结束~` email exactly once after the action returns, bounds it
to 15 seconds, and then attempts `status`, `snapshot`, and `stop` in order.
Email failure or timeout cannot alter the workload or cleanup verdict.

## Review Repair

Concentrated review found one preventable P1 before any long run. A failure
after the Exit observer was started but before the runner armed its observer
exit trap could leave the 26-hour remote capture active. A deterministic
controller test now covers that entry failure. After Mac cleanup, the
controller classifies the observer as:

- `active`: freeze and bundle;
- `inactive`: bundle;
- exact `no observer state`: accept the runner's already-published immutable
  bundle; or
- unknown/unreachable: fail closed.

Success, workload failure, expired sudo, notification failure/hang, active
observer recovery, already-finalized observer, and unproved observer state are
covered. The controller also finds Homebrew `msmtp` at its absolute Apple
Silicon or Intel prefix when a detached shell has a reduced PATH. No
unresolved P0/P1 remains.

## Local Gates

```text
root all-targets:                    713 passed; 3 ignored
main binary:                           2 passed
harness integration:                 10 passed; 4 ignored
root docs / release build:           PASS / PASS
established all-target Clippy:       PASS (existing warnings only)
vendored Quinn:                      40 passed; 3 ignored; doc 1 passed
vendored Quinn integration:           1 expected ignored
vendored Quinn Clippy:               PASS
vendored quinn-proto:               330 passed; docs 3 passed
vendored quinn-proto Clippy:         PASS
frequency reducer/ledger:            PASS / PASS
resource profile/preflight:          PASS / PASS
runner/observer/controller:          PASS / PASS / PASS
market reducer/runner:               PASS / PASS
Knife14 shell helpers:               PASS
Python compile / Bash syntax:        PASS / PASS
fmt / diff / provenance / secret:    PASS / PASS / PASS / PASS
release Endpoint 32MiB:              240.511 Mbit/s
release D16 batch / full-TUN:        PASS / PASS
```

The Endpoint terminal snapshot retained exact conservation at
`61,440/0/0B` available/live/outstanding, with zero socket would-block and
zero abandoned bytes. The D16 batch gate reached `1,569.221 Mbit/s`, with
zero ring-full waits, read errors, terminal drops, or retained ownership.

Two exploratory commands were rejected as non-gates and rerun correctly. A
standalone Quinn manifest does not inherit the root local-proto patch, and an
initial release filter omitted the real module path/`harness` feature and ran
zero tests. The accepted commands used the explicit absolute local proto
patch, verified Cargo metadata provenance, listed the test names, and then ran
nonzero exact gates.

## Decision

The Task-9 local gate is complete. The next transaction is read-only health
and exact-identity admission on Alibaba US candidate 1 and the HK Mac,
followed by one fresh baseline/direct/resource/start/smoke/observer setup and
the first four-epoch run. M3 remains blocked until twelve valid epochs and the
immutable ledger emit `TIER_B_ACCEPTED`.
