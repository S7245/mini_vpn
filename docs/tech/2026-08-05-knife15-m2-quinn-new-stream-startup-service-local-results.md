# Knife15 M2 Quinn New-Stream Startup Service Local Results

Date: 2026-08-05

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS — ONE FRESH M2 REQUIRED**

Architecture:
`docs/tech/2026-08-05-knife15-m2-quinn-new-stream-startup-service-architecture-spec.md`

Failure evidence:
`docs/tech/2026-08-05-knife15-m2-installed-successor-startup-failure-results.md`

## Decision

Exact-source `f570353` artifact
`/tmp/mini_vpn_knife15_macos_20260805_111712.tar.gz` (SHA-256
`b0d3815e...`) fired the auxiliary-replacement stop rule. It passed
baseline/direct, every preflight, seven complete mixed cycles, and cleanup,
then cycle 8 `tcp-forward` delivered `0B` in the first complete Target
receiver interval while the sender had already admitted `2,228,224B`.

The selected transport was installed conn1 generation 2. Gateway/Exit,
routes, interfaces, process, TUN, D16, Endpoint conservation, and exact
transfer completion remained healthy. There were no false generic endpoint
rebinds. This rejects another selector, replacement, pool, pacing, MTU,
window, chunk, Cubic, GSO, workload, or SLI change and selects missing Quinn
new-stream send service.

## Implementation

Every TUIC TCP open now uses one `TuicTcpStartupWriter` module:

1. read the new Quinn stream's original priority;
2. raise it by exactly one before writing TUIC Connect;
3. keep that relative service class armed across empty or blocked writes;
4. on the first accepted nonempty business write, queue the bytes with the
   startup class and restore the exact original priority under the same Quinn
   connection lock;
5. delegate every later write, flush, and shutdown unchanged.

The vendored Quinn/proto seam adds only a hidden atomic
`write_then_set_priority` operation. The pending queue records the startup
priority before the stream's current priority is restored; the driver cannot
run between those operations. A blocked write accepts no bytes and performs
no restoration.

The module owns no payload copy, queue, timer, byte budget, retry, Target
rule, or configuration. Generic join, ordered chunk, unordered reassembly,
native chunk/ordered readers, and D16 direct ordered relay all use the same
writer. Existing send-progress handles, exact writer-pressure ownership,
generation leases, open timeout/errors, and diagnostic metadata remain
attached. UDP paths do not use the module.

When TCP diagnostics are active, the first accepted business write emits one
exact transition:

```text
tuic-tcp-startup-service ... stream=N first_payload_bytes=B
priority_before=P priority_after=Q state=consumed
```

## Deterministic TDD

The proto RED first failed because the atomic operation did not exist. Its
GREEN queues a startup stream ahead of an incumbent and proves that the next
turn rejoins ordinary priority/fairness.

The Quinn RED then failed because `SendStream` exposed no atomic adapter. Its
GREEN uses a real client/server connection, restores priority before the
driver lock is released, and delivers the exact bytes.

The TUIC RED failed because the startup-writer seam did not exist. Its GREEN
uses a relative original priority of seven and proves:

- Connect is written while business authority remains armed;
- empty writes are ordinary and do not consume it;
- a first Pending poll keeps priority eight armed;
- the successful poll queues at eight and restores seven;
- every later payload uses the ordinary writer;
- exactly one positive business admission consumes the authority.

## Capacity and conservation

The exact 32 MiB Endpoint loopback gate passed at:

```text
sender_mbps=240.076
available/live/outstanding=61,440/0/0B
socket_would_block_events=0
```

This remains above the fixed `>170 Mbit/s` architecture gate. The startup
contract only selects an already admitted QUIC STREAM opportunity. Cubic,
flow control, retransmission, and Endpoint pre-accounting remain authoritative:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

## Local gates

```text
TUIC startup tracer:              1 passed
focused proto/Quinn:              1 + 1 passed
root library:                   685 passed, 3 ignored
main binary:                      2 passed
concurrency integration:         10 passed, 4 ignored
release build:                    PASS
all-target Clippy + harness:      PASS (established warnings only)
Knife15 internal/wrapper shell:   PASS
Knife14 shell suites:             PASS
vendored Quinn:                  38 passed, 3 ignored
vendored quinn-proto:           310 passed
vendored docs:                    PASS
Endpoint 32 MiB:                240.076 Mbit/s; 61,440/0/0B
fmt / diff / secret scan:         PASS
```

The first capacity command used a module filter that selected zero tests; it
was explicitly rejected rather than counted and rerun with the exact unique
test name. One proto repair briefly used Rust-2024 let-chain syntax inside an
older-edition vendored crate; the equivalent nested form was applied and the
same gate rerun. Neither event changed product behavior.

## Review

Priority lifetime, aggregate boundedness, scheduler requeue semantics,
blocked/empty/positive writes, cancellation, stream close before payload,
connection-lock ordering, accepted-byte reporting, relative-priority
overflow, error-kind preservation, every relay mode, writer pressure,
progress handles, generation leases, open timeout, diagnostics, UDP
isolation, Endpoint conservation, and frozen values were reviewed.

Review repaired one error-classification regression before full gates: the
atomic adapter now preserves Quinn's established `ConnectionReset` versus
`NotConnected` conversion instead of collapsing errors to `Other`. There are
no unresolved P0/P1 findings.

## Real-Mac discriminator and stop rule

Pull the pushed descendant, rebuild release, keep Clash-TUN and every other
VPN off, and take exactly one fresh:

```text
m2-ipv6-check -> baseline -> direct-discriminator
-> start -> smoke -> m2 -> status -> stop
```

Normal Apple Push/iCloud traffic may remain; avoid intentional heavy
non-test traffic. Preserve `status/snapshot/stop` after a post-start failure.

M2 passes only after the complete 24-hour schedule and cleanup. If startup
consumption is present but an installed successor again produces a
healthy-control complete Target receiver-zero interval, reject this scheduler
contract and open transport-level first-payload ACK/failover research. Do not
tune priority levels, timers, byte budgets, selectors, or frozen values. M3
remains blocked pending full M2 acceptance.
