# Knife15 M2 Prequalification Code Review Local Results

Date: 2026-08-13

Status: **LOCAL REVIEW AND REPAIR PASS; ONE PAIRED M2 QUALIFICATION REQUIRED;
FORMAL M2 AND M3 BLOCKED**

Production/TDD commit: `c06a9d0`.

Runner gate commit: `cce3bf8`.

Preceding architecture result:
`docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-local-results.md`.

## Review Goal

Review the successor proof/install boundary and the complete macOS M2
preflight before spending another real-path run. This stage changes no frozen
workload, SLI, D16, MTU/PLPMTUD, pool, QUIC window, chunk, Cubic, GSO,
Endpoint, retry, or deadline value.

## Findings Repaired

1. **P1 — successor proof/install time-of-check gap.** The production caller
   checked path generation and cwnd before taking the generation-slot install
   mutex. A successor that closed, changed path, or contracted below its typed
   final proof before the install CAS could still become current. The slot now
   samples the live Quinn close/path/cwnd state while it owns the predecessor
   mutex and rejects stale service before changing activity ownership.
2. **P1 — incomplete exact-source cleanliness.** The runner checked only
   staged and unstaged tracked diffs. An untracked `build.rs` or Cargo input
   could therefore affect the release binary while the evidence claimed an
   exact clean source. Both common and M2 action preflights now require empty
   `git status --porcelain=v1 --untracked-files=all`; ignored build output is
   unaffected.
3. **P1 — qualification observer contract was documentation-only.** Formal M2
   admitted an exact healthy observer, but `m2-qualification` did not. A failed
   observer-start command could consume the short discriminator and lose the
   paired attribution needed to decide whether formal M2 was safe. Both modes
   now require the exact tracked observer, matching Exit/Target/ports, healthy
   v2 components, and age at most 900 seconds before traffic.

No unresolved P0/P1 remains in the reviewed successor lifecycle, M2 source
admission, observer admission, TCP/UDP/TUN/D16/Endpoint regression surface, or
cleanup contract.

## TDD Evidence

Expected REDs proved that the old code:

- installed a successor after its path changed;
- installed below its typed final cwnd proof;
- installed after connection close;
- accepted an untracked `build.rs` as exact source; and
- did not require paired observer ownership for qualification.

GREEN coverage adds three reject cases plus one matching live-service install
case, preserves all earlier identity/path/floor CAS tests, and extends the full
runner self-test with source and observer-policy discriminators.

## Complete Gates

```text
root library (single-thread):         713 passed, 3 ignored
main binary:                          2 passed
concurrency integration:             10 passed, 4 ignored
release build:                        PASS
established all-target Clippy:        PASS (existing warnings only)
release full-TUN / D16 batch:         PASS / PASS
runner self-test:                     PASS
observer self-test isolated replay:   PASS
shell / fmt / diff:                   PASS
changed-content secret scan:          PASS
code review:                          PASS; no unresolved P0/P1
```

The exact release 32MiB Endpoint capacity discriminator passed at
`225.382 Mbit/s`, terminal available/live-reservation/outstanding was
`61,440/0/0B`, and socket would-block was zero. The release D16/TUN batch
discriminator measured `2,225.738 Mbit/s` with zero ring drops/full waits.
Both remain above the frozen `170 Mbit/s` stop boundary.

One first observer self-test safely refused a PID-identity race and changed no
remote state; an isolated replay passed. Two initial Endpoint commands used
the wrong module-qualified exact name and ran zero tests; those results were
rejected before the counted exact replay passed.

## Decision And Remaining Work

The runner rejects `b7bb9a9` and requires `c06a9d0` or a descendant. The next
irreducible gate is exactly one paired qualification, about 45 wall-clock
minutes including setup:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> fresh .33 observer start -> m2-qualification -> status -> stop
-> observer freeze/bundle
```

A clean qualification reopens one fresh formal M2, which still needs about 25
uninterrupted hours. Do not run formal first, repeat unchanged, or tune around
a failure. M3 remains blocked until formal M2 and cleanup pass.
