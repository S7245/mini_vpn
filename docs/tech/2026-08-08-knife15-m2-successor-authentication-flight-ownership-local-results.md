# Knife15 M2 Successor Authentication-Flight Ownership Local Results

Date: 2026-08-08

Status: **LOCAL PASS; ONE PAIRED MAC QUALIFICATION AUTHORIZED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `6aefd19`.

Architecture:
`docs/tech/2026-08-08-knife15-m2-successor-authentication-flight-ownership-architecture-spec.md`.

## Code-Review Result

The second review found and resolved two P1 correctness gaps before another
Mac run.

First, TUIC Authenticate is queued and its unidirectional stream is finished
before the successor service turn starts. If Quinn had already emitted that
Data-space packet, the turn began at `sent_bytes=0` and owned only later
carriers. Its exact ACK condition therefore omitted the transport bytes that
establish the successor's protocol precondition.

Second, an already in-flight PLPMTUD probe can also be adopted by the turn.
Quinn deliberately removes a lost MTU probe through a special branch that
does not invoke congestion loss. That branch also omitted the turn's terminal
loss notification, leaving `acked_bytes < sent_bytes` until the shared outer
deadline.

No unresolved P0/P1 remains after the repair and reverse review.

## Implementation

At `start_successor_service_turn`, Quinn now walks the existing Data-space
sent-packet map and adopts only packets that are both ACK-eliciting and
congestion-accounted (`size > 0`). Each packet receives the existing service
tag and its exact encrypted packet bytes enter `sent_bytes` before any later
ACK/loss can be processed.

The turn still snapshots the same current cwnd and emits only enough new
carriers to reach that target. Existing ACK, loss, path-generation, close,
application-limited, Endpoint pacing, and five-second deadline behavior owns
both adopted and newly emitted packets.

The PLPMTUD special-loss branch now invokes the same service-turn
`PacketLost` notification before its unchanged in-flight/MTUD cleanup. It
still does not trigger ordinary congestion loss.

No payload, connection, timer, retry, configuration, allocation, queue, MTU
policy, or frozen value was added or changed.

## Deterministic TDD

Three real Quinn-pair traces lock down the boundary:

```text
pre-turn authentication withheld:
  RED:   start sent_bytes=0
  GREEN: authentication packet is owned; declared loss -> PacketLost

pre-turn authentication delivered:
  GREEN: peer receives exact Authenticate bytes; sent==acked and loss=0

pre-turn PLPMTUD probe withheld:
  RED:   no terminal service-turn event after probe loss
  GREEN: PacketLost with lost_bytes>0
```

The existing realistic-RTT cwnd, ordinary Endpoint-blocked cwnd,
single-owner, exact flight, ordinary loss, path/close, and Endpoint suites
remain green.

## Capacity And Conservation

The exact release 32MiB Endpoint loopback gate passed:

```text
sender capacity:                         240.300 Mbit/s
exact bytes and clean EOF:               PASS
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
vendored quinn-proto:              320 passed; docs 3 passed
root doc compile:                    PASS
fmt/diff/vendor/secret:              PASS
```

The standalone Quinn gate explicitly patched crates.io resolution to the
absolute local quinn-proto path, verified that dependency provenance, and
removed the generated ignored lockfile afterward. Runner hard-timeout output
was its deliberate self-test branch; both the internal runner and wrapper
reached their final PASS markers.

## Reverse Review

- adoption is bounded by Quinn's existing Data-space sent-packet map and
  current congestion authority;
- ACK-only/non-in-flight packets are excluded;
- queued but not-yet-emitted authentication is naturally included in the new
  tagged carrier, while already ACKed authentication needs no adoption;
- old path generations fail through the existing path mismatch result;
- a PLPMTUD loss changes only operation terminal accounting, not MTUD or
  congestion behavior;
- no new hot-path panic, lock, allocation, timer, retry, or unbounded state
  was introduced;
- TCP admission/fallback, UDP, TUN, D16, Endpoint accounting, and predecessor
  drain remain unchanged.

## Qualification Boundary

This review repair prevents a false-ready successor install; it does not prove
the previous short-flow receiver-zero has disappeared. Pull and rebuild the
pushed descendant, then start one fresh bounded `.33` observer and take
exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2, repeat the
same qualification, or tune unchanged. Formal M2 and M3 remain blocked.
