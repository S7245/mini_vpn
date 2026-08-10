# Knife15 M2 Transport-Write-Demand Flight Ownership Local Results

Date: 2026-08-10

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Implementation commit: `c218d8a`.

Architecture:
`docs/tech/2026-08-10-knife15-m2-transport-write-demand-flight-ownership-architecture-spec.md`.

Failure evidence:
`docs/tech/2026-08-10-knife15-m2-transport-write-demand-flight-qualification-failure-results.md`.

## Result

Paired exact-source `b437b93` evidence selects a client-to-Exit
write-progress/worker-turn congestion-ownership hole. The Mac passed
baseline/direct, smoke, every preflight, seven mixed phases, and cleanup, then
cycle 2 short forward lost one complete Target receiver interval. The paired
Exit received about `4.23MB`, supplied without a gap above `165.430ms`, and
received Target ACKs at about `1..4ms` with no retransmit growth. Formal M2 was
not run.

## Implementation

Each Quinn send stream now keeps an exclusive offset boundary for the exact
prefix accepted from a writer whose continuous demand was already proven by
Blocked. A successful Writable retry extends that boundary and clears only the
waiting-for-retry bit. If the D16 task then yields while reporting progress,
the connection worker can packetize those bytes and the resulting packets
still carry the stream's demand ownership.

Cumulative ACK removes the boundary after its exact prefix settles. Finish
retains outstanding prefix ownership; reset, STOP_SENDING, terminal error, and
0-RTT rollback clear it. The O(1) aggregate counts only live boundaries.
Packets for another stream, control traffic, and later same-stream bytes above
a completed boundary cannot borrow authority. No allocation, queue, timer,
wake, retry, connection, or frozen value changed.

## TDD

The production-order replay is:

```text
Blocked -> Writable -> nonempty retry -> connection worker turn -> Blocked
```

With `82ms` one-way latency, production Endpoint policy, a `128KiB` send
window, and `2MiB` payload:

```text
old implementation: initial 24,000B -> final 1,772,034B
required bound:                         at least 1,990,080B
new implementation:                    PASS bound
```

Additional GREEN tests preserve the earlier Writable-before-retry behavior,
two-stream reset isolation, deferred-writer cross-stream isolation, and exact
cumulative-ACK cleanup so later same-stream bytes remain unowned.

## Capacity And Gates

The exact release 32MiB Endpoint loopback gate passed:

```text
sender capacity:                         239.784 Mbit/s
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
Knife15/Knife14 shell:              PASS
vendored Quinn:                     40 passed, 3 ignored; doc 1 passed
vendored Quinn integration:          1 expected ignored
vendored quinn-proto:              325 passed; docs 3 passed
vendored quinn-proto Clippy:        PASS (default feature lane)
root doc compile:                    PASS
root fmt and narrow diff:            PASS
```

The standalone Quinn gate verified the absolute local quinn-proto path and
removed its generated ignored lockfile. One exploratory `--all-features`
proto Clippy selected optional AWS-LC FIPS and failed because local `cmake` is
absent; it is not an established product lane. Default-feature proto Clippy
passed. One initially mistyped exact capacity module ran zero tests and was
rejected before the correctly qualified one-test gate above.

## Review

Review checked cumulative and reordered ACK behavior, packet-range boundary
semantics, finish/reset/STOP/0-RTT cleanup, aggregate underflow, multi-stream
isolation, same-stream post-boundary isolation, hot-path cost, and frozen-path
reachability. No unresolved P0/P1 remains.

## Qualification Boundary

Pull/rebuild the pushed reviewed descendant, then start one fresh bounded
`.33` observer only when the Mac is ready. Take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2, repeat
unchanged, or tune constants. A repeated receiver-zero interval with exact
offset-flight ownership rejects this architecture.
