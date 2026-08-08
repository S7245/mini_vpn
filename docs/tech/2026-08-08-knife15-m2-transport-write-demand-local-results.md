# Knife15 M2 Transport-Write-Demand Ownership Local Results

Date: 2026-08-08

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `f9c3c23`.

Failure evidence:
`docs/tech/2026-08-08-knife15-m2-transport-write-demand-qualification-failure-results.md`.

Architecture:
`docs/tech/2026-08-08-knife15-m2-transport-write-demand-architecture-spec.md`.

## Artifact Result

Exact-source `eb2185f` artifact
`/tmp/mini_vpn_knife15_macos_20260808_092053.tar.gz` (SHA-256
`100d3163...`) passed baseline `17.521/65.771 Mbit/s`, bounded direct at
`8.758 Mbit/s` without a complete zero interval, smoke
`67.438/46.284 Mbit/s`, every preflight, the long forward/reverse TCP and
reverse UDP phases, and cleanup. Its cycle-1 short forward then admitted
`10,092,544B` at the sender but delivered `4,194,304B` to the Target, with two
sender-zero intervals and one complete receiver-zero interval. Formal M2 was
not run.

The short stream used replacement generation 2, installed after a successful
service turn at `24,886B` cwnd and about `165ms` RTT. Quinn accepted
`6,300,119B`, the writer remained Pending for up to `2,367,062us`, and the
last sampled QUIC ACK was only `3,849,795B`; Target bytes had already exceeded
that snapshot. D16, TUN, Endpoint conservation, process, routes, and cleanup
remained healthy. No fresh paired Exit observer existed, so no historical
capture is presented as exact paired evidence.

## Implementation

Each live Quinn send stream now owns one bounded demand bit after its
application write reaches `WriteError::Blocked`. Writable delivery does not
clear it; a nonempty retry, finish, reset, STOP_SENDING, or 0-RTT rollback
does. `StreamsState` keeps one aggregate count only as an O(1) fast reject.

When Quinn emits STREAM frames, only the packet that actually carries a
demand-owning stream receives `transport_write_demand=true`. Its later ACK can
grow Cubic even if an empty transmit poll has since published the ordinary
connection-wide application-limited snapshot. Control traffic and other
streams cannot borrow that ownership. No allocation, queue, timer, wake,
retry, new connection, or frozen value was added.

Separately, successor pre-start adoption excludes the exact active PLPMTUD
probe packet. Authentication STREAM packets remain inside exact readiness
ACK/loss ownership. The unchanged MTUD state machine still records probe
loss, but a protocol-independent old probe can no longer reject an otherwise
qualified successor.

## Deterministic TDD And Review Repair

The realistic Quinn pair uses `82ms` one-way latency, the production Endpoint
policy, a `128KiB` send window, and a `2MiB` business stream:

```text
old path:  initial cwnd 24,000B -> final 151,200B
new path:  initial cwnd 24,000B -> final 2,044,424B
```

Code review then found one P1 before commit: making any live blocked writer
override the whole connection's application-limited state let a cancelled or
deferred writer lend congestion authority to an unrelated stream. The new
cross-stream RED grew the unrelated stream's cwnd from `146,658B` to
`213,998B`. Per-packet STREAM ownership made it remain exactly `146,658B`.

Additional tests prove two blocked streams retain independent ownership across
one reset, an old MTU probe loss remains MTUD-owned while the service turn
succeeds, and ordinary authentication loss still fails closed. Reverse review
found no remaining P0/P1.

## Capacity And Complete Gates

The exact release 32 MiB Endpoint loopback gate passed:

```text
sender capacity:                         240.079 Mbit/s
exact bytes and clean EOF:               PASS
socket would-block:                      0
available/live reservation/outstanding: 61,440/0/0B
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
vendored quinn-proto:              323 passed; docs 3 passed
root doc compile:                    PASS
fmt/diff/vendor/secret:              PASS
```

The standalone Quinn gate used the absolute local quinn-proto patch, verified
dependency provenance, and removed its generated ignored lockfile. The runner
hard-timeout message was its deliberate self-test branch and reached the final
PASS marker.

## Qualification Boundary

This repair is intended to be sufficient for the exact blocked-writer
short-flow discriminator, not for formal M2. Start a fresh bounded `.33`
observer only when the Mac is ready, pull/rebuild the pushed descendant, and
take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2, repeat
the same qualification, or tune unchanged. Formal M2 and M3 remain blocked.
