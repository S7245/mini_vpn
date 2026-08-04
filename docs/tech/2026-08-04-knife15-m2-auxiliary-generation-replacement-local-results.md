# Knife15 M2 Auxiliary Generation Replacement Local Results

Date: 2026-08-04

Status: **LOCAL IMPLEMENTATION ACCEPTED — one fresh real-Mac M2 required**

## 1. Selected Evidence And Change

Exact-source `fd6c34f` artifact
`/tmp/mini_vpn_knife15_macos_20260804_095117.tar.gz` (SHA-256
`889cdd276c...`) passed baseline/direct, start/smoke, IPv6/full-tunnel and
real-client prerequisites, but failed the first short forward with one full
Target receiver-zero interval. Busy-epoch qualification correctly isolated
degraded conn1 (`black_holes 0 -> 16`); both opens used qualified conn0, whose
data writer then waited `1,169,717us` behind fourteen existing lease halves.
This falsified the selector-only architecture.

Commit `5533d15` replaces a proven degraded busy auxiliary generation without
resetting its existing streams:

- each logical slot owns its connection, generation identity, exact
  generation activity, write pressure, open state, and auth evidence;
- an authenticated successor is installed atomically as the only new-open
  generation, and the triggering reservation binds to it;
- the predecessor becomes drain-only and closes only after its exact
  generation activity reaches zero;
- conn0 remains primary and is never replaced; UDP, heartbeat, health, and
  primary reconnect remain on conn0;
- the configured/eligible pool remains two, with at most one non-admitting
  predecessor and three live QUIC generations during bounded overlap;
- generic and D16 native opens consume the same generation-bound selection;
- endpoint recovery samples current and draining writer pressure under their
  exact transport identities.

Commit `addc54d` independently makes formal M2 UDP phases reject loss above
the existing `3.0%` limit immediately. Exact `3.0%` remains accepted, and
diagnostic continuation behavior is unchanged.

## 2. Invariants And Capacity

The change adds O(1) work only at TCP-open/replacement boundaries. It adds no
per-byte or per-packet work and changes no D16, MTU, window, chunk, Cubic, GSO,
self-wake, Endpoint pacing, workload, duration, or SLO value.

The frozen endpoint conservation law remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

The exact 32 MiB Endpoint gate reached `239.967 Mbit/s` and ended at
`61,440/0/0B`, with zero socket would-block events. This remains sufficient
for the frozen M2 single-flow rates even while three generations share the
one endpoint-owned wire budget.

## 3. TDD And Review

Focused generation/admission coverage proves:

- qualified conn0 `active14` plus degraded conn1 `active6` requests
  replacement and reserves only the installed successor;
- predecessor and successor lease counters are isolated, including the exact
  zero notification race;
- primary, stale-predecessor, invalid-successor, and second-predecessor
  installs fail closed;
- concurrent replacement ownership is bounded and a fresh successor skips
  the stale-idle probe;
- lease splitting is fallible rather than panicking on saturation;
- existing least-active/path-service/stable and all-degraded fallback rules
  remain covered.

Review found and repaired lost zero-transition wake-up, concurrent global
replacement permit handling, predecessor reap identity mismatch, hot-path
lease clone saturation, and immediate stale probing of a fresh successor.
There are no unresolved P0/P1 findings.

## 4. Local Gates

```text
TCP-pool focused:                 31 passed
root library:                    671 passed, 3 ignored
main binary:                       2 passed
concurrency integration:          10 passed, 4 ignored
release build:                    PASS
all-target Clippy + harness:      PASS (established warnings only)
Knife15 internal/external shell:  PASS
Knife14 shell suites:             PASS
vendored Quinn:                   37 passed, 3 ignored; doc 1 passed
vendored quinn-proto:            309 passed; docs 3 passed
Endpoint 32 MiB:                 239.967 Mbit/s; 61,440/0/0B
fmt / diff / secret scan:         PASS
code review:                      PASS; no unresolved P0/P1
```

## 5. Real-Mac Acceptance And Stop Rule

Take exactly one fresh transaction from the pushed reviewed descendant with a
rebuilt release binary and every other VPN disabled:

```text
m2-ipv6-check -> baseline -> direct-discriminator
-> start -> smoke -> m2 -> status -> stop
```

On any post-start failure preserve `status -> snapshot -> stop`. The run must
show replacement start/install for the comparable degraded-auxiliary case,
zero complete Target receiver intervals, exact bounded predecessor drain (or
still-owned bounded evidence), unchanged Endpoint/D16 conservation, UDP loss
at or below `3%`, and complete route/DNS/process cleanup.

Do not tune or repeat unchanged. If an installed successor still produces a
healthy-control complete receiver-zero interval, reject this architecture and
open Quinn initial-stream scheduling/failover research. An independent UDP
loss above `3%` remains its own fail-closed branch. M3 stays blocked until a
complete M2 plus cleanup PASS.
