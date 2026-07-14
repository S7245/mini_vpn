# Knife14h10d16 TUN Ingress Service Local Gate Results

Date: 2026-07-13
Base: `d934f124a54caf1a60963c7ff84bf619a73b2262`
Verdict: **LOCAL PASS; later frozen VPS P1 disproved architecture sufficiency**

## Implemented Seam

The H10d16-only TUN adapter now splits the asynchronous TUN device into a
continuous reader pump and the actor-owned writer. The reader feeds a bounded
FIFO whose capacity comes from the already frozen `tun_tx_queue_len` estimate.
The actor retains raw DNS/UDP/SYN classification, stages TCP packets in FIFO
order, then performs one smoltcp poll, one TUN flush, and one dirty-relay pass
per existing bounded batch.

The default non-H10 adapter remains direct and unsplit. MTU, kernel queue,
48/240 drain bounds, D16, pool, QUIC windows, chunk, Cubic, GSO, endpoint
pacing, driver work, and self-wake were not changed.

## Focused TDD

The initial eight-packet tracer produced the expected RED:

```text
TCP packets:          8
smoltcp poll entries: 8
TUN flush calls:      8
dirty-relay entries:  1
```

After separating packet preparation from batch service, the same tracer is
GREEN:

```text
TCP packets:          8
TCP batches:          1
smoltcp poll entries: 1
TUN flush calls:      1
dirty-relay entries:  1
avoided polls:        7
```

Pump tests prove FIFO order, capacity-one full-wait accounting, clean EOF,
one terminal read error, consumer-close termination, and no task panic. A
full event-loop test also proves terminal TUN failure exits instead of
repeatedly selecting a closed FIFO.

## Exact 32 MiB Gate

The final real-Quinn forward tracer used the frozen `64 KiB` application
write, MTU `1200`, a bounded 500-packet modeled kernel ring, a 500-packet
ingress FIFO, H10d16 byte ownership, and EndpointWindowV1.

```text
receiver throughput:              293.488 Mbit/s
payload elapsed:                  914.637167 ms
sent / received:                  33,554,432B / 33,554,432B
pattern errors:                   0
ACK / clean EOF:                  yes / yes
modeled kernel-ring drops:        0
kernel-ring high water:           38 / 500 packets
pump FIFO high water:             56 / 500 packets
pump full waits / read errors:    0 / 0
TCP packets:                      28,933
TCP batches:                      11,817
batch iface polls / flushes:      11,817 / 11,817
avoided dirty-relay passes:       17,116
avoided iface polls:              17,116
D16 terminal ownership/tail:      zero / clean
```

The strict `>170 Mbit/s` local architecture discriminator passes with about
72% headroom, and neither bounded ingress layer reaches its capacity.

## Regression Gates

- mini_vpn root library: `629` nonignored PASS, `3` expected ignored;
- harness library: `640` nonignored PASS, `3` expected ignored;
- integration suite: `10/10`, `4` expected ignored;
- explicit TCP concurrency: `64/64`, `256/256`, `1024/1024`;
- UDP sweep: `500/500` exact at `1000B`, `1400B`, fragmented `4000B`, and
  fragmented `8000B`;
- vendored quinn-proto: `309/309` unit and `3/3` doc;
- vendored Quinn with explicit local proto patch: `29/29` nonignored,
  `3` expected ignored, and `1/1` doc;
- `cargo check --all-targets --features harness`: PASS without warnings;
- root fmt, both runner syntax/self-tests, and diff checks: PASS.

The old 32-flow profiler fixture was also corrected. A comparison at exact
`d934f12` showed that both baseline and loaded phases could time out while the
test still compared fractions. The profiler self-test now uses one completed
flow, preserves the multi-thread runtime and `300us` synthetic poll cost,
requires `1/1` completion in both phases, and passed five consecutive runs.
Concurrency remains covered by the dedicated sweeps.

## Code Review

Review verified FIFO ordering, raw/staged/pump ownership separation, bounded
capacity, H10-only reachability, default direct behavior, DNS/UDP bypass,
SYN-before-poll, read-error/EOF/consumer-close termination, actor-owned TUN
writes, D16 and endpoint conservation, and aggregate-only observability.

One P1 was found and fixed: `run_event_loop` previously ignored terminal TUN
wait errors, which could busy-wake after the pump channel closed. It now logs
one terminal cause and exits fail-closed. No unresolved P0/P1 remains.

No macOS TUN test ran.

## VPS Outcome And Stop Rule

The exact commit later retained `194 Mbit/s` receiver and exact service
equalities on VPS, but reported `419` TUN TX drops, pump high water `500/500`,
and `347` full waits. The declared architecture-failure rule fired. Do not
tune constants to rescue it; see the sibling VPS results document.
