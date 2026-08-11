# Knife15 M2 Successor Forward-Service Inheritance Local Results

Date: 2026-08-11

Status: **LOCAL IMPLEMENTATION, CAPACITY, AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION IS REQUIRED BEFORE FORMAL M2; M3 REMAINS BLOCKED**

Reviewed implementation: `de4d170`

Formal failure evidence:
`docs/tech/2026-08-10-knife15-m2-successor-forward-service-inheritance-formal-failure-results.md`.

Architecture:
`docs/tech/2026-08-10-knife15-m2-successor-forward-service-inheritance-architecture-spec.md`.

## Result

The formal M2 artifact passed five complete cycles and most of cycle 6, then
lost the first Target receiver interval of `short-forward-3`. The exact
predecessor had surrendered a `41,301B` congestion window through its one-shot
path-state reset, while its replacement installed after one service turn at
only `26,424B`. The previous one-turn sufficiency claim is rejected.

`de4d170` preserves the exact positive pre-reset cwnd as monotonic state owned
by that auxiliary transport identity and logical generation. A replacement
still performs at least one existing exact successor service turn. When the
predecessor owns a floor, it performs sequential current-cwnd turns on the
same path until current cwnd reaches that floor. Handshake and every round
share the unchanged five-second whole-replacement deadline. Loss, path
change, connection close, deadline, or a successful round without cwnd
progress below the floor fails closed.

The reset operation now holds the generation-slot mutex across identity
classification, floor publication, `path_changed()`, and the after-snapshot.
The install CAS independently rechecks the successor's final proved floor
against the latest slot-owned predecessor requirement. Therefore neither a
stale recovery handle nor a proof/install race can transfer a lower service
generation into current ownership.

No Target request or business payload exists before install. Pool size,
predecessor drain, attempt-local qualified fallback, initial/Unknown
availability, Ready certificate anti-churn, D16, Endpoint pacing, MTU,
windows, chunk, Cubic, GSO, self-wake, workload, and SLIs remain unchanged.

## TDD Evidence

Focused scalar tests prove:

- positive exact-generation floor publication and read;
- monotonic max under later lower evidence;
- reconnect invalidation;
- formal values `26,424B < 41,301B`, then completion above the dynamic floor;
- no-progress classification is terminal;
- install rejects a `26,424B` successor after the slot requirement advances
  to `41,301B`, while a `52,848B` proof installs.

A real two-connection Quinn pair proves current, draining, and stale reset
classification through the exact slot seam. A second real pair sets a dynamic
floor at three initial windows, requires at least two sequential exact turns,
ACKs every tagged byte with zero loss, remains on one exact path generation,
and reaches the floor before the shared deadline.

## Capacity And Full Gates

```text
root library:                       705 passed, 3 ignored
main binary:                        2 passed
concurrency integration:            10 passed, 4 ignored
release build:                      PASS
established all-target Clippy:      PASS (existing warnings only)
Knife15 runner/wrapper shell:       PASS
Knife14/Exit observer shell:        PASS
root doc compile:                   PASS
vendored Quinn:                     40 passed, 3 ignored
vendored Quinn integration:         1 expected ignored
vendored Quinn docs / Clippy:       1 passed / PASS
vendored quinn-proto:               330 passed
vendored quinn-proto docs / Clippy: 3 passed / PASS
root fmt / diff / patch provenance: PASS
changed-content secret scan:        PASS
code review:                        PASS; no unresolved P0/P1
```

Standalone Quinn provenance was verified before its gates:

```text
quinn-proto v0.11.16
  (/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/third_party/quinn-proto-0.11.16)
└── quinn v0.11.11
```

The exact 32MiB release capacity gate passed at `240.349 Mbit/s`:

```text
Endpoint rate/burst:         30,720,000 B/s / 61,440B
available/live/outstanding:  61,440/0/0B
socket would-block events:   0
```

This remains above the unchanged `>170 Mbit/s` architecture discriminator and
preserves the Endpoint conservation invariant.

## Review Repairs And Invalid Commands

Review first rejected a non-atomic reset sequence that published the floor,
released the slot lock, and only then reset Quinn. The accepted implementation
makes ownership publication and reset one slot transaction. Review then found
that the floor read before handshake could theoretically be superseded before
install; the accepted install CAS rechecks the latest requirement and has a
focused regression test.

Two focused `cargo test --exact` commands initially omitted the
module-qualified test name and ran zero tests. They were rejected and rerun
with exact names; the full suite independently ran all of them. One earlier
integration command omitted `--features harness` and was likewise rejected
before the corrected `10+4 ignored` gate. A whole-module rustfmt check recursed
into unchanged vendored Quinn siblings and reported pre-existing style
differences; only the two narrow maintained fork hunks were reviewed, while
root fmt and diff checks passed. Generated standalone lockfiles and targets
were removed.

## Qualification Boundary

Do not run another 25-hour formal M2 yet. Pull and rebuild the pushed reviewed
descendant, start one fresh bounded `.33` Exit observer, and take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2-qualification -> status -> stop
```

The run normally schedules about thirty minutes and can only return
`PASS_NON_ACCEPTANCE`. Preserve `status/snapshot/stop` after any post-start
failure. A receiver-zero after a logged nonzero inherited floor rejects this
mechanism as sufficient; do not tune or repeat unchanged. If qualification and
cleanup pass, formal M2 is reopened on the same production code and frozen
workload. M3 remains blocked until formal M2 passes.
