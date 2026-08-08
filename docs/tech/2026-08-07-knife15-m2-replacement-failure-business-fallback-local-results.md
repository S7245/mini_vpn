# Knife15 M2 Replacement-Failure Business Fallback Local Results

Date: 2026-08-07

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `68c7271`.

Failure source:
`docs/tech/2026-08-07-knife15-m2-successor-service-turn-qualification-failure-results.md`.

Architecture:
`docs/tech/2026-08-07-knife15-m2-replacement-failure-business-fallback-architecture-spec.md`.

## Implementation

`TcpPoolAdmission` now accepts an attempt-local exclusion mask and an optional
qualified-only gate. Normal admission remains byte-for-byte equivalent through
its test-only wrapper. After one auxiliary replacement failure,
`TuicUpstream::acquire_tcp_pool_reservation`:

1. stores the exact failed slot and error;
2. excludes that slot for the current business open;
3. resamples only already-qualified current generations;
4. reserves one through the unchanged preparation/activity/lease path;
5. returns the combined maintenance/admission error if no safe current
   generation exists.

Because post-failure admission filters every degraded and unknown candidate,
the same open cannot initiate replacement on a second auxiliary slot. The
attempt-local state is dropped on return, so a later independent open retains
fresh replacement authority. Replacement success, successor CAS, predecessor
drain, existing flows, and all frozen values are unchanged.

## Focused TDD

The first test failed to compile because the exclusion seam did not exist. The
minimal two-slot implementation passed. Code review then identified the
multi-slot second-replacement edge; a three-slot extension produced the
expected RED, and qualified-only admission produced GREEN.

The final replay proves:

```text
first degraded auxiliary:          ReplaceAuxiliary(slot 1)
failed slot exclusion:             PASS
second degraded auxiliary retry:   FORBIDDEN
qualified fallback:                ReserveCurrent(slot 0)
preparation concurrency:           Busy
ownership before/held/after:        6/7/6
later independent replacement:     ReplaceAuxiliary(slot 1)
```

Focused final gates passed:

```text
exact fallback replay:              1 passed
TCP pool suite:                     37 passed
Endpoint recovery:                 16 passed
successor service turn:             2 passed
```

## Capacity And Complete Gates

The final exact 32 MiB Endpoint loopback gate produced:

```text
sender:                       240.470 Mbit/s
available/live/outstanding:   61,440/0/0B
socket would-block:           0
exact bytes, clean EOF:       PASS
```

The unchanged conservation invariant remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

Complete gates:

```text
root library:                     689 passed, 3 ignored
main binary:                        2 passed
concurrency integration:           10 passed, 4 ignored
release build:                      PASS
established all-target Clippy:      PASS (existing warnings only)
Knife15 internal/wrapper shell:     PASS
Knife14/Exit observer shell:        PASS
vendored Quinn:                     40 passed, 3 ignored; doc 1 passed
vendored Quinn integration:          1 expected ignored
vendored quinn-proto:              315 passed; docs 3 passed
root doc compile:                    PASS
fmt/diff/vendor/secret:              PASS
```

## Review

Review covered missed wakes, preparation and draining-permit drop, activity
CAS, lease totals, failed-slot re-entry, all-degraded fallback, pool lengths
greater than two, later-open authority, replacement success, terminal errors,
bounded state, logs, credentials, and TCP/UDP/TUN/D16/Endpoint regressions.

The review found and repaired one P1 before commit: a mask alone could permit
the same open to replace a different degraded auxiliary in a larger pool.
Qualified-only post-failure admission closes that path and also prevents
unknown evidence from being presented as a safe fallback. There are no
unresolved P0/P1 findings.

## Qualification Boundary

The previous observer is stopped and bundled. Start a new bounded observer on
`.33` only when the Mac is ready, then pull/rebuild the pushed descendant and
take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2 or repeat
or tune unchanged. Qualification can only produce `PASS_NON_ACCEPTANCE`; it
must show either the repaired qualified fallback with connected phase service
or an exact new discriminator before any further architecture.
