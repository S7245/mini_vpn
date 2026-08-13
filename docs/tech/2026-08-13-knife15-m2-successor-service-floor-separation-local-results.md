# Knife15 M2 Successor Service Floor Separation Local Results

Date: 2026-08-13

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED M2 QUALIFICATION
REQUIRED; FORMAL M2 AND M3 BLOCKED**

Production/TDD commit: `b7bb9a9`.

Architecture:
`docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-architecture-spec.md`.

Formal failure:
`docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-formal-failure-results.md`.

## Implemented Contract

- `readiness_cwnd_floor` captures the positive cwnd after the first exact
  ACK-owned successor service turn.
- Additional turns may reach the predecessor's larger handoff requirement,
  but their final cwnd is carried in a typed install proof and is not persisted
  as the installed generation's lifetime readiness floor.
- The typed proof binds exact successor identity, path generation, readiness
  floor, and final cwnd. The slot install CAS rechecks it under the predecessor
  mutex against the latest handoff requirement.
- Each replacement preparation overwrites the active transaction requirement
  with `max(immutable readiness floor, exact current Quinn cwnd)`. A failed
  historical `3.6MB` proof cannot ratchet a later `381,502B` generation back
  to `3.6MB`.
- Same-path `17,360 < 24,800B` remains `StaleCwnd`; identity/path mismatch
  remains stale; current `361,778B` still overrides a `26,338B` readiness
  baseline for a legitimate handoff.

No D16, MTU/PLPMTUD, pool, QUIC window, chunk, Cubic, GSO, Endpoint,
self-wake, workload, SLI, retry, or deadline value changed. The new logic runs
only during already-authorized auxiliary replacement, not on the business
payload hot path.

## TDD And Review

Expected REDs proved that the old model lacked a first-turn readiness value,
accepted only a naked install scalar, and monotonically retained the failed
`3,605,919B` transaction when a later current sample was `381,502B`.

GREEN coverage now proves:

- first-turn readiness remains distinct from a multi-round final proof;
- exact artifact state reserves the Ready current generation;
- a failed historical handoff cannot poison the next attempt;
- install retains only readiness as the future baseline;
- wrong successor path/readiness proof is rejected;
- insufficient latest handoff proof is rejected;
- the prior `17,360/24,800B` and `361,778/26,338B` discriminators remain
  load-bearing.

Concentrated review repaired one robustness issue before acceptance: the
install interface now consumes a proof bound to the exact forward-service
result rather than a naked `u64`. No unresolved P0/P1 remains across identity,
path, mutex/preparation ownership, failure fallback, predecessor drain,
TCP/UDP/TUN/D16/Endpoint behavior, or test sensitivity.

## Complete Gates

```text
root library (single-thread gate): 709 passed, 3 ignored
main binary:                         2 passed
concurrency integration:            10 passed, 4 ignored
release build:                       PASS
established all-target Clippy:       PASS (existing warnings only)
vendored Quinn:                      40 passed, 3 ignored; doc 1 passed
vendored quinn-proto:               330 passed; docs 3 passed
vendored default Clippy lanes:       PASS
root docs:                           PASS
release full-TUN / D16 batch:        PASS / PASS
runner / observer self-tests:        PASS / PASS
shell / fmt / diff / provenance:     PASS
changed-content secret scan:         PASS
code review:                         PASS; no unresolved P0/P1
```

The exact release 32MiB Endpoint discriminator passed twice after review at
`240.330` and `240.256 Mbit/s`. Final
available/live-reservation/outstanding was `61,440/0/0B`; socket would-block
was zero. This remains above the frozen `170 Mbit/s` stop boundary.

One default-parallel full-suite run temporarily measured `168.781 Mbit/s` with
a `35.65ms` local scheduling gap. The exact isolated replay immediately
returned `240.256 Mbit/s`, and the complete single-thread suite passed. This
was classified as local test contention; no constant or production path was
changed.

The first standalone Quinn command omitted the absolute local proto patch and
failed on fork-only APIs. The corrected command verified the local
`third_party/quinn-proto-0.11.16` path and passed. An exploratory
`--all-features` Quinn Clippy then requested the unavailable FIPS CMake build;
it is not the established gate. Default Quinn and quinn-proto Clippy passed.
The observer self-test also safely refused one PID-identity race, then passed
on isolated replay; it did not modify remote state.

## Decision And Next Gate

The runner now rejects `642e3ae` and requires `b7bb9a9` or a descendant. Pull,
rebuild, and take exactly one fresh paired:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> fresh .33 observer start -> m2-qualification -> status -> stop
-> observer freeze/bundle
```

Do not run formal M2 first and do not repeat/tune unchanged. A clean
qualification reopens one fresh formal M2; recurrence rejects the separation
as sufficient. M3 remains blocked.
