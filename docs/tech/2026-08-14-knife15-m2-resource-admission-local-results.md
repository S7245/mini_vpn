# Knife15 M2 Resource Admission Local Results

Date: 2026-08-14

Status: **PASS — TASK 2 COMPLETE AT `c688ce3`; TASK 3 IS NEXT; FORMAL M2 AND
M3 REMAIN BLOCKED**

## Outcome

The Tier-A resource identity and read-only preflight seam is implemented.
This stage does not modify Rust production code, the root-owned macOS TUN
runner, the strict formal `m2` receiver-zero rule, any workload constant, or a
frozen data-plane value.

`scripts/knife15-m2-resource-profile.py` now validates one exact closed JSON
schema, canonicalizes it for SHA-256 ownership, and classifies a candidate
against the `.33` reference. It accepts only:

- a different provider **and** ASN;
- an independently contracted route class and contract identity; or
- a same-route replacement with hash-bound proof of both prior saturation and
  replacement capacity.

It rejects the same candidate/Exit, equivalent unsaturated resizing,
malformed/non-global IPv4, invalid ports/ASN/hashes, missing or unknown fields,
unsafe tokens, inconsistent saturation claims, and Target mismatch.

`scripts/knife15-m2-resource-preflight.sh` is intentionally separate from the
root/TUN runner. Its production action:

- refuses sudo and non-system network-tool overrides;
- requires a clean tracked worktree and an exact nonsymlinked release binary,
  direct manifest, SSH key, and reference/candidate profiles;
- copies the profiles before classification so later input mutation cannot
  change the admitted identity;
- binds provider, route-contract, and optional saturation/capacity evidence by
  SHA-256;
- matches source, release binary, direct profile, observer, TUIC endpoint,
  Target, SSH identity, and physical interface;
- records bounded local route/traceroute and remote service/resource evidence;
- verifies the exact sing-box binary/config hashes and UDP listener;
- scans copied evidence for credential-like assignments; and
- seals an internal checksum inventory plus an immutable tar SHA-256.

No production preflight has run because no new candidate VPS/resource profile
has been selected. The repository fixtures are test-only and cannot be used as
real resource evidence.

## TDD Evidence

The implementation proceeded as vertical RED/GREEN slices:

1. missing classifier -> distinct provider/ASN fixture GREEN;
2. equivalent resize wrong reason -> exact ineligibility GREEN;
3. missing server-config hash accepted -> closed schema GREEN;
4. malformed SHA-256 accepted -> exact lowercase digest validation GREEN;
5. malformed public IPv4 accepted -> globally routable IPv4/port/ASN GREEN;
6. arbitrary saturation strings accepted -> paired, exact digest proof GREEN;
7. newline-bearing candidate ID accepted -> bounded safe token GREEN;
8. option-shaped SSH destination accepted -> exact user/IPv4 grammar GREEN;
9. unbound identity/saturation evidence -> exact copied-file hash binding
   GREEN.

## Local Gates

PASS:

```text
bash -n scripts/knife15-m2-resource-preflight.sh
bash scripts/knife15-m2-resource-preflight.sh --self-test
/usr/bin/python3 -I scripts/knife15-m2-resource-profile.py --self-test
/usr/bin/python3 -m py_compile scripts/knife15-m2-resource-profile.py
bash scripts/knife15-macos-soak.sh --self-test
git diff --check
focused credential-assignment scan
```

The complete unchanged macOS runner self-test exited `0` and printed
`knife15 macOS runner self-test passed`. Its intentional hard-timeout fixture
still prints `ERROR: command exceeded hard timeout of 1s`; that is expected
test evidence, not a runner failure.

## Code Review

The concentrated review covered schema ambiguity, equivalent-resource false
admission, arbitrary proof hashes, SSH option injection, shell/remote command
injection, input TOCTOU, symlinks, tool substitution, Python environment
poisoning, credential leakage, evidence integrity, and formal-runner
isolation.

The review repaired three pre-commit risks:

- strict SSH destination grammar rejects option-shaped input;
- classification and all reads operate on the copied evidence profiles;
- provider/route identity and saturation replacement claims must match copied
  evidence files rather than merely naming a digest.

No unresolved P0/P1 remains. One operational prerequisite remains: an actual
candidate resource, sanitized provider/route evidence, and exact profile must
be created before this preflight can produce a real PASS.

## Next

Implement Task 3 from
`docs/tech/2026-08-14-knife15-m2-tiered-continuity-resource-strategy-implementation-plan.md`:
bind strict `m2-qualification` and `m2` to the admitted candidate/profile hash
without changing the strict receiver-zero SLI or any frozen value.
