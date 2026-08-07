# Knife15 M2 Service-Normalized Admission Local Results

Date: 2026-08-07

Status: **LOCAL GATES PASS; ONE MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation: `6e78d23` (`fix(knife15): normalize busy pool admission by
service`)

Failure source:
`docs/tech/2026-08-07-knife15-m2-cold-pool-placement-qualification-failure-results.md`.

Architecture:
`docs/tech/2026-08-07-knife15-m2-service-normalized-admission-architecture-spec.md`.

## Outcome

`TcpPoolAdmission` now evaluates active current-generation ownership against
the current service of every admitted busy candidate:

```text
normalized_load = active_lease_ownership * RTT / cwnd
```

Forward qualification and bounded auxiliary replacement retain categorical
precedence. Normalized ordering is enabled only when every admitted candidate
is busy and has a known positive service sample. Idle, unknown, and explicit
all-degraded states retain the old least-active/equal-busy-service/stable
fallback. Exact normalized ties use lower raw ownership, greater service, and
stable index in that order.

The comparison uses an overflow-safe continued-fraction ordering of positive
`u128` rationals. It adds no floating point, saturation, threshold, weight,
timer, allocation, dependency, state, or configuration. Generic and D16 open
paths receive the same reservation decision; no connection, stream, payload,
recovery action, or frozen value changed.

## TDD

The corrected exact-source replay releases only control preparation while
retaining its live lease, matching `live_tcp_conn`. On unmodified `12e845f`
it failed solely at the data decision:

```text
actual:   conn0 active_before=2
expected: conn1 active_before=3
```

The implementation selects conn1. Focused pool coverage is `34/34`, including:

- the warm-control then data replay;
- exact normalized ties and deterministic raw-load fallback;
- 65,536 small rational comparisons against safe cross products;
- three-factor max-`u64` values whose naive cross product exceeds `u128`;
- invalid-zero and unknown fail-closed behavior;
- idle, all-degraded, forward-qualification, replacement-generation,
  preparation, and CAS behavior;
- generic/D16 selection diagnostics.

The diagnostic line now reports
`service_normalized_ordering=true|false`, selected active/cwnd/RTT, and every
candidate's exact admission evidence.

## Capacity And Full Gates

The exact 32 MiB Endpoint capacity gate produced:

```text
sender:                       240.313 Mbit/s
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
root library:                     686 passed, 3 ignored
main binary:                        2 passed
concurrency integration:           10 passed, 4 ignored
release build:                      PASS
established all-target Clippy:      PASS (existing warnings only)
Knife15 internal/wrapper shell:     PASS
Knife14/Exit observer shell:        PASS
vendored Quinn:                     40 passed, 3 ignored; doc 1 passed
vendored Quinn integration:          1 expected ignored
vendored quinn-proto:              311 passed; docs 3 passed
root doc compile:                    PASS
fmt/diff/vendor/secret:              PASS
```

Code review found two repairable issues: a private invalid `Known` sample
could have reached division by zero if a future constructor bypassed
`TcpPoolPathService::known`, and the documentation did not explicitly preserve
the prior selector falsifiers. Invalid samples now return `None`, and the spec
states why degraded-lane collapse and installed-successor startup behavior are
unchanged. No unresolved P0/P1 remains.

## Qualification Boundary

The paired Exit observer is active on `.33`:

```text
/tmp/mini_vpn_knife15_exit_target_observer_20260807_064333
```

Pull and rebuild the pushed descendant, then run exactly one fresh:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2-qualification -> status -> stop
```

The run normally schedules about thirty minutes and can only produce
`PASS_NON_ACCEPTANCE`. Preserve `status/snapshot/stop` after failure. Do not
run formal M2 or repeat/tune unchanged.

The next result is decisive:

- normalized ordering reached, warm qualified path selected, no zero
  interval: retain this architecture as a healthy differential comparator;
- normalized ordering reached and the same zero interval recurs: reject it as
  insufficient without tuning;
- normalized ordering not reached because evidence is idle or unknown:
  classify an admission-coverage mismatch rather than success.
