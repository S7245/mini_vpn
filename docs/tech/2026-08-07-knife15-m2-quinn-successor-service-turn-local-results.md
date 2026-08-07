# Knife15 M2 Quinn Successor Service Turn Local Results

Date: 2026-08-07

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `541fbfb`.

Failure source:
`docs/tech/2026-08-07-knife15-m2-successor-service-readiness-qualification-failure-results.md`.

Architecture:
`docs/tech/2026-08-07-knife15-m2-quinn-successor-service-turn-architecture-spec.md`.

## Selected Failure

Exact-source `7131de4` artifact
`/tmp/mini_vpn_knife15_macos_20260807_091815.tar.gz` (SHA-256
`7c749fb9...`) passed baseline `35.336/61.978 Mbit/s`, the 300-second direct
discriminator at `17.659 Mbit/s` without gaps, start/smoke, every preflight,
the long forward/reverse TCP/reverse UDP phases, and cleanup. Reverse UDP loss
was `1.873567%`.

The first short forward then lost one complete Target receiver interval. The
Mac admitted `10,092,544B`, the Target received `4,063,232B`, and the exact
D16 writer ultimately acknowledged `5,893,598B`. Endpoint maximum/final was
`61,440B / 61,403/0/0B`; TUN, routes, process, interface, D16 terminal
ownership, and cleanup remained healthy.

Service-normalized admission ran correctly. After the control reservation,
both data candidates owned two leases; exact `active * RTT / cwnd` ordering
selected conn1 generation 2 at `12,000B/163.342ms` over conn0 at
`8,400B/163.153ms`. Conn1 generation 2 was the authenticated auxiliary
successor installed after its service-proven predecessor had reached
`780,994B/164.107ms`. The successor had not supplied forward bulk and still
owned only its initial window. This rejects normalized placement as
sufficient and selects authentication-only successor installation.

The intended `.33` observer had already reached its two-hour hard timeout and
captured no matching packet. The exact client lifecycle supports the selected
installation-contract fix; it does not retroactively manufacture paired Exit
evidence.

## Implementation

The bounded auxiliary replacement now sequences:

```text
QUIC/TUIC authenticate
  -> one current-cwnd successor service turn
  -> every tagged packet byte ACKed on the same path generation
  -> existing identity/activity/generation CAS install
```

quinn-proto owns one O(1) state machine. It snapshots the current positive
congestion window, emits valid ACK-eliciting `PING + PADDING` carriers, tags
the exact encrypted congestion-controlled packet sizes, and drives the
existing Endpoint pacing path as `Bulk`. The first carrier may also contain
already queued Authenticate control STREAM bytes. Success requires
`sent >= target`, `acked == sent`, zero tagged loss, and the original path
generation. Tagged loss, path change, or close is terminal and has no retry.

The hidden Quinn adapter permits one waiter, forwards exact success/failure,
wakes on connection termination, and exposes a read-only partial snapshot for
the deadline owner. TUIC reuses the original single five-second replacement
deadline for handshake plus service turn; it adds no timer or constant.
Failure leaves the predecessor current and creates no draining generation or
admission transfer. Success still must pass the unchanged slot identity and
activity CAS.

Diagnostics preserve the old `handshake_ms` meaning and separately report
`service_turn_ms` and whole `replacement_ms`. Success includes target/sent/
acked/lost bytes and path generation; deadline error includes the exact
partial snapshot available at expiry. No payload or secret is logged.

## Focused TDD

The initial protocol and Quinn adapter tests failed to compile with the
service-turn API absent. Minimal implementation then passed:

```text
root Quinn adapter ACK flight / one owner:       2 passed
generation slot lifecycle/CAS:                   3 passed
service-normalized admission replay:             1 passed
Endpoint recovery regression:                   16 passed
quinn-proto state / ACK / loss / snapshot:        4 passed
```

The deterministic ACK replay proves every tagged byte is acknowledged and
the fresh congestion window grows. The deterministic loss replay drops one
tagged packet and terminates with exact
`sent=11,616B, acked=10,164B, lost=1,452B`; it does not retry or report
success. State coverage also locks one-owner, close, path-change, Endpoint
bulk classification, and terminal state release.

## Capacity And Full Gates

The exact 32 MiB Endpoint gate produced:

```text
sender:                       240.291 Mbit/s
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
root library:                     688 passed, 3 ignored
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

Code review found one P1 observability regression: the old `handshake_ms`
field initially included service-turn duration, while deadline errors omitted
partial turn counters. Separate timing fields and the read-only active-turn
snapshot repaired both before commit. Review of cancellation, late ACK,
loss, close, path generation, Endpoint debt, successor CAS, predecessor
ownership, TCP/UDP/TUN/D16 behavior, and secrets found no unresolved P0/P1.

## Qualification Boundary

This mechanism is intended to be sufficient only for the exact cold-successor
first-receiver-interval discriminator. It is not formal-M2 acceptance or a
general bandwidth promise.

A fresh `.33` observer is active at:

```text
/tmp/mini_vpn_knife15_exit_target_observer_20260807_102734
```

It started with both tcpdump and TCP_INFO sampler live under the bounded
two-hour hard timeout. Pull and rebuild the pushed descendant and take exactly
one Mac transaction while this observer remains active:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2, repeat an
unchanged qualification, increase the service turn, or tune any frozen value.
If the turn completes and the installed successor still produces the same
receiver-zero discriminator, reject this architecture and return to paired
client/Exit evidence.
