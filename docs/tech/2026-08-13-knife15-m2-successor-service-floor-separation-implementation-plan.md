# Knife15 M2 Successor Service Floor Separation Implementation Plan

Date: 2026-08-13

Status: **COMPLETE; ONE PAIRED M2 QUALIFICATION REQUIRED**

Architecture:
`docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-architecture-spec.md`.

## Frozen Scope

Do not change D16, MTU/PLPMTUD, pool size, QUIC windows, chunk, Cubic, GSO,
Endpoint rate/burst, self-wake, recovery bounds, workload, SLI, runner timeout,
or observer behavior. Do not add retries or relax failure fallback.

## Vertical Tasks

1. Add one focused proof-model RED: a multi-round successor proof must expose
   its first-turn readiness floor separately from its larger final handoff
   proof.
2. GREEN the proof with one positive `readiness_cwnd_floor` captured after the
   first exact ACK-owned turn; retain the final cwnd and all existing
   ACK/loss/path/deadline behavior.
3. Add one generation-install RED: a successor constructed with a lower
   readiness floor and a higher final handoff proof must satisfy predecessor
   install while retaining only the readiness floor as its future baseline.
4. GREEN by passing the transaction-only final proof explicitly into the
   generation-slot install CAS instead of persisting it in the installed
   generation.
5. Add the exact artifact admission RED: current `381,502B`, first-turn floor
   below it, same identity/path, zero black-hole advance, and historical final
   proof above it must remain Ready and reserve current.
6. GREEN by constructing the certificate from the first-turn floor. Do not
   weaken `StaleCwnd`, `StaleIdentity`, or `StalePath` classification.
7. Preserve and rerun the existing `17,360/24,800B`, exact-current handoff,
   latest-floor install recheck, proof loss, path mismatch, replacement
   fallback, predecessor drain, and reconnect regressions.
8. Extend diagnostics with both `readiness_cwnd_floor` and
   `final_cwnd`; keep existing log keys where compatibility matters.
9. Run focused tests after each slice, then root library/main/integration,
   release, established Clippy, vendored Quinn/quinn-proto, docs, shell/self
   tests, fmt/diff/provenance/secret checks, and the exact 32MiB capacity gate.
10. Run concentrated code review for correctness, concurrency, hot-path cost,
    TCP/UDP/TUN/D16/Endpoint regressions, and test sensitivity. Repair any P0/P1
    before continuing.
11. Record the formal failure, local result, reusable learning/error, and the
    next paired-qualification boundary in `HANDOFF.md`, `TODO.md`, `AGENTS.md`,
    and `CONTEXT.md`.
12. Commit coherent production/test work, commit documentation/memory, push the
    reviewed descendant, and provide the exact Mac qualification transaction.

## Stop Rules

- Expected focused REDs may enter their minimum GREEN implementation.
- Any unexpected regression or repair failure requires root-cause analysis
  before further edits.
- A local 32MiB result at or below `170 Mbit/s` is an architecture failure; do
  not tune constants.
- No Mac formal M2 is authorized by local success. One clean paired
  qualification is the next effectiveness gate; M3 remains blocked.
