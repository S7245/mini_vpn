# Knife15 M2 Endpoint-Blocked App-Limited Ownership Local Results

Date: 2026-08-08

Status: **LOCAL PASS; ONE PAIRED MAC QUALIFICATION AUTHORIZED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `1d08565`.

Failure evidence:
`docs/tech/2026-08-08-knife15-m2-endpoint-blocked-app-limited-qualification-failure-results.md`.

Architecture:
`docs/tech/2026-08-08-knife15-m2-endpoint-blocked-app-limited-architecture-spec.md`.

## Result

The paired Mac and Exit evidence selected one integration-seam ownership
defect. Quinn's ordinary application-limited snapshot treated an empty
transmit poll as idle when continuously queued STREAM data was blocked only
by the Endpoint bulk reservation service. The later ordinary ACK could then
skip Cubic slow-start growth.

The repair records only that exact Endpoint `Bulk` block for the current poll:

```text
app_limited = buf.is_empty()
              && !congestion_blocked
              && !endpoint_bulk_blocked
```

Control-only Endpoint waits, true application idle, Quinn pacing/congestion,
default-off behavior, Cubic, per-packet successor tags, loss/path/close,
Endpoint accounting, and every frozen product value are unchanged. The
change adds no field, timer, retry, connection, payload, allocation, or
unbounded state.

## TDD

A real Quinn pair uses `82ms` one-way delay, one datagram of Endpoint bulk
availability, disabled test MTUD, and a continuously queued 64KiB STREAM.

```text
RED:   initial_cwnd=12,000B final_cwnd=12,000B wait_count=120
GREEN: Endpoint waits > 0 and final_cwnd >= 24,000B
```

This proves at least one complete ordinary slow-start growth round survives
Endpoint-blocked empty polls. It does not use the tagged successor flight and
therefore closes the business-packet gap exposed by the qualification.

## Capacity And Conservation

The exact release 32MiB Endpoint loopback gate passed:

```text
sender capacity:                         239.487 Mbit/s
exact bytes and EOF:                     PASS
socket would-block:                      0
available/live reservation/outstanding: 61,440/0/0B
```

The unchanged conservation invariant remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

## Complete Gates

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
vendored quinn-proto:              317 passed; docs 3 passed
root doc compile:                    PASS
fmt/diff/vendor/secret:              PASS
```

One root full-suite run saw the old 1ms/8ms/1ms loop-profiler sleep test
descheduled during its park segment (`7.0875ms`). The same test then passed
twenty serial runs and the complete 692-test suite passed; no production or
VPN assertion was changed. A standalone vendored Quinn invocation that
selected registry quinn-proto, a harness command without `--features
harness`, and an exact capacity command with an accidental `--ignored` were
rejected as invalid zero/provenance gates and rerun correctly.

## Review

Review traced empty, partial-batch, Quinn pacing, congestion, Endpoint Bulk,
Endpoint Control, MTU probe, idle, loss, path-change, close, and tagged
successor cases. `endpoint_bulk_blocked` is local to one poll and can become
true only immediately before that poll breaks on a denied Bulk reservation.
A partial datagram already makes `buf.is_empty()` false; Quinn pacing already
sets `congestion_blocked`; Control waits cannot set the new cause. No
unresolved P0/P1 remains.

## Qualification Boundary

This repair is intended to be sufficient only for the exact cold-successor
first-128KiB receiver-interval discriminator. It is not a formal-M2 result.
Start one fresh bounded `.33` observer and take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2 or repeat
or tune unchanged. The paired result must retain or reject this architecture
before any next change.
