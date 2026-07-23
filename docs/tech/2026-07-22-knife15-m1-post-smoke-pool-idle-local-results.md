# Knife15 M1 post-smoke TCP-pool idle barrier local results

Date: 2026-07-22

## Result

Exact-source `f60926e` HK bundle
`/tmp/mini_vpn_knife15_macos_20260722_110059.tar.gz` (SHA-256
`9071ec0fb8bf832714000bddf14a843714536aeaa422217c47b89bab2532ceca`)
was operated correctly. Fresh baseline was `24.371186/30.882152 Mbit/s` and
the 300-second direct prerequisite passed at `12.180742 Mbit/s` with every
complete Target receiver interval positive.

The first M1 forward phase offered `12.185593 Mbit/s`. The local sender wrote
`457,048,064B`, but the Target received `456,523,776B`; its first complete
receiver interval was exactly zero. The final partial interval was positive,
so this is not the repaired command-tail observer class. The user preserved
status/snapshot/stop evidence. Stopping about fourteen hours later did not
cause a failure already recorded five minutes after M1 start.

Gateway loss, physical interface errors, TUN drops, Endpoint rebinds, resource
growth, and cleanup failures were all zero. Endpoint conservation held at or
below `61,440B` and ended `59,040/0/0B`. Same-stream ACK progress correctly
suppressed the previous false-rebind loop.

## Selected root

Smoke completed at `11:01:45Z`; M1 was prepared in that same second and began
at `11:01:46Z`. Its control stream selected conn0 from idle. The closing smoke
reverse data flow still owned both native half leases on conn1, so both pool
slots then had two leases. Stable least-active tie-breaking put the new M1
data stream on conn0 too. The old conn1 flow was reaped only after this open.

Three comparable earlier HK starts had drained smoke before the first M1 data
open and placed control/data on conn0/conn1. Their first Target receiver
intervals were positive. This selects a smoke-to-formal-workload lifecycle
race; it does not authorize a pool, timer, pacing, or SLO change.

## Repair

The existing 250ms Endpoint monitor now publishes
`tuic-tcp-pool-activity active_leases=N` only when the exact pool lease total
changes. Native reader and writer halves retain independent leases, so zero
proves that both halves of every smoke relay have dropped.

The macOS runner waits after smoke for the latest exact zero, bounded by the
existing smoke hard timeout (`DURATION + 30s`). Missing, malformed, or nonzero
evidence fails closed and leaves the TUN running for status/snapshot/stop.
Formal M0/M1 independently require the latest total to remain zero before
creating workload evidence. No fixed grace sleep, interval waiver, or frozen
data-plane/workload change was introduced.

## TDD and review

The focused Rust RED failed because the transition publisher did not exist.
The shell RED failed because no idle-evidence parser existed. GREEN proves
transition-only publication and fail-closed missing/nonzero/malformed parsing.

Review covered half-lease lifetime, atomic sampling, monitor lock failure,
stale activity records, parser anchoring, log compaction, timeout behavior,
failure evidence preservation, idle-to-first-open races, hot-path log volume,
and all frozen values. During a formal target-only smoke, each TCP flow remains
active for 20 seconds, so the 250ms monitor cannot miss the nonzero episode;
after smoke no new opener competes with the drain barrier. No unresolved P0/P1
remains.

## Final local gates

```text
focused pool activity            1/1
root all-targets                 666 passed / 3 ignored; main 2/2
integration harness              10 passed / 4 ignored
32 MiB EndpointWindowV1          240.322 Mbit/s; final 61,440/0/0B
vendored Quinn                   37 passed / 3 ignored; doc 1/1
vendored quinn-proto             309/309; doc 3/3
cargo build --release            PASS
cargo clippy --all-targets       PASS (established warnings only)
Knife15 runner + wrapper         PASS
Knife14 three shell self-tests   PASS
shell syntax / root fmt / diff   PASS
changed-content secret scan      PASS
code review                      no unresolved P0/P1
```

The Knife15 self-test's printed one-second timeout is its expected negative
fixture and the suite exited successfully. Standalone Quinn used the required
absolute local quinn-proto patch and executed a nonzero test count.

## Accepted stop position

The observed smoke-to-M1 race is locally repaired, but the failed partial run
is not M1 acceptance. Next use the pushed repair for one fresh user-operated
HK M1 with a rebuilt release and fresh baseline/direct artifacts. Keep every
other VPN/TUN disabled through `stop`; preserve status/snapshot/stop on any
failure. Do not tune frozen values or waive TCP/UDP SLIs. M2/M3 remain blocked
until a complete M1 passes.
