# Knife15 M2 Successor Service-Turn App-Limited Ownership Local Results

Date: 2026-08-08

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `c3c264a`.

Failure source:
`docs/tech/2026-08-08-knife15-m2-successor-service-turn-app-limited-qualification-failure-results.md`.

Architecture:
`docs/tech/2026-08-08-knife15-m2-successor-service-turn-app-limited-architecture-spec.md`.

## Implementation

`Connection::on_packet_acked` now preserves non-application-limited
classification only when the exact stored `SentPacket` carries the existing
`successor_service_turn` tag. A later empty transmit poll can no longer erase
the send-time ownership of that deliberately full congestion-window flight.
For every untagged packet the congestion-controller argument is identical to
the previous implementation.

Tagged loss, path change, connection close, exact byte settlement, one-owner
state, and the shared five-second replacement deadline remain unchanged. The
change adds no state, timer, retry, payload, Target connection, parameter, or
configuration surface.

## Focused TDD

The first weak assertion (`final_cwnd > initial_cwnd`) passed and was rejected
because it could not prove the missing invariant. The final real Quinn pair
uses `82ms` one-way latency and requires:

```text
successful exact turn
final_cwnd >= initial_cwnd + tagged_acked_bytes
```

Before the implementation it failed at
`12,000 + 13,068 > 23,616`. The minimal tagged-ACK correction made it GREEN.
All five successor-turn protocol tests passed, including exact ACK, loss/path
failure, one flight, realistic RTT, and terminal loss. The two mini_vpn Quinn
adapter tests and the exact replacement/fallback replay also passed.

## Capacity And Complete Gates

The exact 32 MiB Endpoint gate produced:

```text
sender:                       237.701 Mbit/s
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
vendored quinn-proto:              316 passed; docs 3 passed
root doc compile:                    PASS
fmt/diff/vendor/secret:              PASS
```

The independent Quinn manifest initially resolved registry quinn-proto and
correctly failed on missing fork APIs. The result was rejected; the final gate
used the explicit absolute local patch, verified the dependency tree, and then
produced the counts above. Its generated ignored lockfile was removed.

## Review

Review covered ordinary versus tagged ACK classification, delayed and late
ACKs, Cubic slow-start semantics, tagged loss, path generation, close,
cancellation, the service-turn one-owner boundary, replacement CAS,
predecessor lifecycle, Endpoint debt, and TCP/UDP/TUN/D16 regressions.

The changed expression is behavior-identical for every untagged packet. A
failed tagged flight remains ineligible for successor install even though an
eventual ACK retains its correct packet-level congestion meaning. No
unbounded state, secret, hot-path allocation, or new P0/P1 finding remains.

## Qualification Boundary

The capacity gate predicts sufficiency only for the exact cold-successor
first 128KiB receiver interval. With about `0.5s` spent opening the control and
data streams and a `164ms` RTT, the broken `13.28KiB` window can supply only
about `92.96KiB` in three rounds, while the corrected at-least `24.8KiB`
window can supply about `173.6KiB`.

Start a fresh bounded `.33` observer only when the Mac is ready, pull and
rebuild the pushed descendant, then take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2 or repeat
or tune unchanged. A corrected installed cwnd plus another receiver-zero
interval rejects this architecture; otherwise the paired evidence must supply
a new exact discriminator before any next change.
