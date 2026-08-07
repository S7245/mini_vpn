# Knife15 M2 Ordered-Gap Path Migration Local Results

Date: 2026-08-06

Status: **LOCAL PASS; FORMAL M2 REMAINS BLOCKED PENDING TWO-CYCLE MAC QUALIFICATION**

Architecture:
`docs/tech/2026-08-06-knife15-m2-ordered-gap-path-migration-architecture-spec.md`.

Failure evidence:
`docs/tech/2026-08-06-knife15-m2-ordered-gap-path-migration-failure-results.md`.

## Result

The implementation exposes exact ordered receive progress without placing
recovery policy in Quinn. A live D16 reader registers a cloneable read-only
progress handle in its owning pool generation. The existing `250ms` recovery
sampler observes the registry; the payload read path acquires no recovery
lock and allocates no diagnostic payload.

One Endpoint rebind becomes eligible only when all of these remain exact for
at least one full existing sampler interval and two observations:

```text
stable QUIC identity
reader identity
QUIC stream identity
application-consumed read offset
lowest buffered offset strictly after the missing prefix
nonzero buffered ordered gap
active TCP workload
```

Authority is covered before socket I/O. Read progress, a changed gap, stream
release, inactive workload, or connection-generation replacement clears the
accumulated observation. The existing hard writer ACK-stall Endpoint rebind
retains precedence. A later greater-offset gap can own one new episode.

The action reuses the existing Endpoint UDP socket rebind. It preserves the
QUIC connection, TUIC stream, Target TCP connection, payload, pool, D16
ownership, and previous-socket authentication/drain lifecycle. No payload is
replayed, copied into a recovery queue, reset, or moved to another connection.

## Quinn Mechanism Boundary

The patched proto snapshot reports:

- application-consumed ordered prefix;
- lowest buffered chunk offset;
- highest observed stream offset;
- assembler buffered bytes;
- exact missing-prefix gap bytes.

The high-level `RecvStreamProgress` handle performs only a read-only stream
state lookup. Empty and contiguous assemblers report no gap; consumed state
returns zero debt; released/closed streams return `ClosedStream`. Quinn has no
timer, Endpoint reference, recovery threshold, or TUIC policy.

A live Quinn regression opens a stream before Endpoint rebind, consumes its
prefix, rebinds to a new UDP socket, and receives the suffix on the same
connection and same stream. This locks down the payload-preserving migration
mechanism required by the architecture.

## Runner

Public `m2-qualification` now runs exactly two formal mixed cycles:

```text
2 × (300s forward TCP + 300s reverse TCP + 180s reverse UDP + 10s short forward)
```

Including DNS, real-client probes, controlled checks, and command overhead it
normally takes about thirty minutes. Success is always
`PASS_NON_ACCEPTANCE`; formal M2 acceptance remains `NOT_RUN`.

The runner now validates exactly `8` phase results (`6` TCP and `2` UDP), two
DNS results, two cycle-indexed real-client results, zero complete receiver
intervals, the unchanged `16MiB` TCP gap and `3%` UDP loss limits, Endpoint
final debt/conservation, D16 terminal ownership, and complete Endpoint rebind
lifecycle. An unrecovered or failed migration cannot receive the qualification
verdict. `status/snapshot/stop` evidence remains available after any failure.

Review found and repaired two fail-closed gaps before declaring local PASS:

1. two recovery turns could occur less than `250ms` apart after a delayed
   sampler tick, and inactive samples could retain authority; eligibility now
   requires the full existing interval and inactive ownership clears;
2. the old one-cycle qualification labeled its real-client artifact
   `qualification_cycle_001`, which the cycle envelope did not count, and did
   not run an exact final qualification SLO; labels are now `cycle_001/002`
   inside the qualification-specific directory and the final envelope is
   mandatory.

No unresolved P0/P1 remains.

## Capacity Gate

The exact `32 MiB` Endpoint loopback gate delivered exact bytes and EOF at:

```text
sender_mbps                         = 240.403
socket_would_block_events           = 0
available/live/outstanding final    = 61,440/0/0B
```

This exceeds the unchanged `>170 Mbit/s` stop rule. The conservation law is
unchanged:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

The implementation adds no payload queue, pacing reservation, permit, timer
constant, or copy, so the failed `29.973634 Mbit/s` workload remains well
inside the accepted capacity path.

## Local Gates

```text
focused Endpoint recovery policy:  19 passed
read-pressure registry lifecycle:   1 passed
root library:                      684 passed, 3 ignored
main binary:                         2 passed
concurrency integration:            10 passed, 4 ignored
release build:                       PASS
all-target Clippy + harness:         PASS (established warnings only)
Knife15 internal/wrapper shell:      PASS
Knife14/observer shell suites:       PASS
vendored Quinn:                     40 passed, 3 ignored; docs 1 passed
vendored quinn-proto:              311 passed; docs 3 passed
root docs:                           PASS
fmt/diff/patch/secret checks:        PASS
```

The first two capacity commands incorrectly added `--ignored` to a test that
is not ignored and therefore selected zero tests. They were rejected as
non-gates; the exact unique test without `--ignored` produced the result
above. A test-log wrapper also used zsh's reserved `status` variable, and one
new shell validator initially called a nonexistent helper. During the final
gate pass, two stale test path/target names, a missing harness feature, and an
over-strict Clippy invocation were also rejected rather than counted as
gates. All command issues were corrected and their exact established lanes
were rerun with the expected nonzero test counts.

## Architecture Scores

- Clean Architecture: **10/10**. Quinn exports mechanism-only read state;
  TUIC owns detection policy; the existing Endpoint adapter owns migration.
- DDIA fault tolerance: **10/10 locally**. Evidence, active ownership,
  elapsed sampling, episode identity, pre-I/O cover, deterministic choice,
  old-socket retention, result validation, and failure preservation are
  bounded and fail closed. Real-WAN liveness is the remaining qualification,
  not missing local architecture.
- Deep-module seam: **10/10**. One small progress API hides the assembler and
  neither transport internals nor runner policy leak into D16.

## Next Mac Discriminator

Pull the pushed reviewed descendant, rebuild release, keep Clash-TUN and every
other VPN off, and take exactly one fresh transaction:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2-qualification -> status -> stop
```

Do not run formal `m2` yet. Qualification normally takes about thirty minutes.
Preserve `status/snapshot/stop` after failure. Classification is:

- ordered-gap migration applied and no receiver-zero interval: retain the
  architecture;
- migration applied and receiver-zero recurs: reject it without tuning;
- receiver-zero recurs but no exact migration is eligible: reject the
  observer/policy seam;
- no ordered gap and no receiver-zero: healthy comparator; cleanup PASS is
  still required before deciding whether formal M2 may proceed.
