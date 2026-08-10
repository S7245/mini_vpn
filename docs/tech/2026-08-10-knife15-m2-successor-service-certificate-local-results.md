# Knife15 M2 Successor Service Certificate Local Results

Date: 2026-08-10

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION IS REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `3737dee` (`fix(tuic): persist successor service readiness`)

Architecture:
`docs/tech/2026-08-10-knife15-m2-successor-service-certificate-architecture-spec.md`.

Failure evidence:
`docs/tech/2026-08-10-knife15-m2-successor-service-certificate-qualification-failure-results.md`.

## Implemented Contract

Each fresh auxiliary generation now stores an immutable optional service
certificate only after its existing exact successor service turn succeeds.
The certificate owns the exact stable transport identity, logical pool
generation, ACK-owned path generation, and positive post-turn cwnd floor.

Before a new TCP open, the Quinn adapter supplies scalar current identity,
path generation, cwnd/RTT, and black-hole evidence. Admission classifies the
certificate as `Ready`, `StaleIdentity`, `StalePath`, `StaleCwnd`, or
`Unknown`. A stale auxiliary is excluded while a non-stale current lane
exists and uses the existing bounded fresh-generation replacement seam. An
uncertified initial generation and incomplete adapter evidence remain
available as `Unknown`; all-stale and replacement-unavailable cases preserve
bounded current-generation fallback.

Reconnect clears the old certificate before the logical identity advances.
Replacement failure remains attempt-local and cannot start a second
maintenance action for the same business open. Existing streams drain on the
predecessor unchanged; only later opens can use the installed successor.

No D16, MTU/PLPMTUD, pool, QUIC window, chunk, Cubic, GSO, Endpoint,
self-wake, retry, workload, or SLI value changed.

## RED/GREEN Evidence

- Exact Mac state `proved_floor=24,800B`, `current_cwnd=17,360B`, same
  identity/path, and zero black holes was RED because the old policy reserved
  the stale successor. It is GREEN as one `ReplaceAuxiliary` decision.
- Equality at `24,800B` remains `Ready` and does not churn. Identity/path
  mismatch is stale; missing current path generation remains `Unknown`.
- All-stale, unavailable replacement, predecessor drain, failed replacement,
  reconnect, certificate transfer, and zero-floor rejection boundaries pass.
- The realistic Quinn replay uses `82ms` one-way latency, completes a fresh
  successor service turn, drops one ordinary business packet, and delivers
  the first `128KiB` receiver interval within one second.

## Code Review

Review found one bounded-progress defect before commit. A stale busy
auxiliary without an exact qualification epoch could be selected for
replacement and then skipped inside the decision loop, allowing the same
candidate to be selected indefinitely. Replacement selection now requires
the exact identity/anchor/current epoch before it can enter maintenance; a
deterministic regression requires current-generation fallback instead of
spin. No unresolved P0/P1 remains.

Review also rechecked categorical ordering, all-stale availability,
same-open failure fallback, predecessor ownership, reconnect invalidation,
scalar dependency direction, hot-path allocation risk, and unchanged
TCP/UDP/TUN/D16/Endpoint behavior.

## Gates

- root library: `700 passed; 3 ignored`;
- main: `2 passed`;
- concurrency harness: `10 passed; 4 ignored`;
- release build and established repository Clippy lane: PASS;
- Knife15/Knife14/control shell syntax and self-tests: PASS;
- vendored Quinn with the exact local proto patch: `40 passed; 3 ignored`,
  integration `1 ignored`, doc `1 passed`;
- vendored quinn-proto: `326 passed`, docs `3 passed`, default Clippy PASS;
- root docs and the full D16/TUN 32MiB release gate: PASS;
- exact 32MiB Endpoint gate: `237.868 Mbit/s`, final
  available/live/outstanding `61,440/0/0B`, zero socket would-block;
- fmt, diff, narrow vendor, generated-file, and changed-content secret checks:
  PASS.

An exploratory `-D warnings` Clippy invocation promoted established warnings
in untouched code to errors on the current toolchain. It is not the accepted
repository lane; the established Clippy command passed and no warning was
introduced by this stage.

## Qualification Boundary

Pull and rebuild the pushed reviewed descendant. When the Mac is ready, start
one fresh bounded `.33` Exit observer and take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` evidence after a failure. Do not run formal
`m2` or repeat/tune unchanged. A repeated complete Target receiver-zero
interval rejects this certificate architecture; a clean qualification can
only produce `PASS_NON_ACCEPTANCE` and unlock the next formal-M2 decision.
