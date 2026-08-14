# Knife15 M2 Strict Resource Attempt Ledger Local Results

Date: 2026-08-14

Status: **PASS; TASK 4 COMPLETE; TASK 5 RUNBOOK AND FULL LOCAL GATES NEXT;
NO MAC/VPS QUALIFICATION HAS RUN**

## Outcome

The bounded Tier-A policy is now executable as an immutable decision ledger.
It cannot accept the historical `.33` Exit as a new candidate, cannot admit a
formal run before qualification, cannot start candidate 2 before candidate 1
is genuinely rejected, and cannot accept a candidate until two consecutive
valid strict formal runs pass.

The ledger emits only:

- `TIER_A_PENDING`;
- `TIER_A_ACCEPTED`; or
- `TIER_A_EXHAUSTED`.

No Rust production code, strict `receiver_zero_intervals == 0` requirement,
workload constant, TUIC/QUIC parameter, D16/Endpoint behavior, or M2 SLI
changed.

## Implementation

Reviewed implementation commits:

- `a52b048` — closed ledger/parser, evidence fixtures, automatic attempt
  sealing, and frozen historical-reference identity;
- `ffd99af` — strict qualification/formal source admission and ledger source
  validation require `a52b048` or a descendant.

The ledger validates before reducing a valid attempt:

- external Mac and Exit bundle SHA-256;
- safe single-root tar structure and immutable resource directory/archive
  equality;
- source, release binary, runner, profile helper, preflight runner, observer,
  server binary, and server configuration hashes;
- canonical per-run resource profile plus stable material resource identity;
- exact workload contract, including all derived offered rates and frozen
  phase/count fields;
- candidate endpoint, Target, ports, observer coverage, nonempty packet
  capture, zero kernel drops, result integrity, safety, and cleanup;
- exact qualification/formal stage start and terminal timestamps; and
- strict TCP receiver-zero and UDP loss summaries.

Each run must carry a fresh resource-evidence profile, but consecutive valid
runs must keep one stable resource identity and the same source, binary,
workload contract, server, observer, endpoint, and Target. This separates
fresh direct/route observations from candidate configuration drift.

Invalid environment/operator/power/VPS evidence neither creates a candidate
state nor changes completed formal-pass state. Genuine receiver-zero, UDP,
safety, or lifecycle evidence rejects the exact candidate. A later valid run
after rejection or acceptance fails closed.

## Review Repairs Before Long Testing

Concentrated review found and repaired four locally preventable risks:

1. `/tmp` is normally a system symlink on macOS. The CLI now resolves the
   artifact root once while still rejecting symlinked bundle files.
2. A structurally valid but drifted historical reference could have authorized
   a false resource comparison. The helper now requires the frozen `.33`
   failure-domain identity and `.77` Target.
3. The first reducer allowed candidate 2 to start while candidate 1 was still
   pending. Candidate order is now serialized.
4. Invalid attempts originally participated in valid profile/Exit uniqueness
   checks. They are now audit-only and cannot erase or block valid evidence.

Archive path ambiguity, counter coverage, observer lifecycle timestamp order,
source/tool provenance, contract drift, evidence tampering, duplicate resource
identity, post-terminal attempts, and duplicate JSON keys have deterministic
negative coverage. Review has no unresolved P0/P1.

## Gates

PASS:

```text
/usr/bin/python3 -I scripts/knife15-m2-continuity-ledger.py --self-test
/usr/bin/python3 -I scripts/knife15-m2-resource-profile.py --self-test
bash scripts/knife15-m2-resource-preflight.sh --self-test
bash scripts/knife15-exit-target-observer.sh --self-test
bash scripts/knife15-macos-soak.sh --self-test
/usr/bin/python3 -I scripts/knife15-m2-continuity-ledger.py evaluate \
  scripts/fixtures/knife15-m2-ledger/ledger-template.json \
  --artifact-root /tmp
git diff --check
```

The complete runner self-test includes its expected hard-timeout fixture line
and terminates with `passfailpassknife15 macOS runner self-test passed`.

## Next

Task 5 must now provide the exact operator runbook and complete repository
build/test/lint/provenance/secret review gates. Do not start candidate traffic
until Task 5 closes. Afterward, admit candidate 1 through the read-only
resource preflight and execute only the ledger-authorized qualification/formal
sequence.
