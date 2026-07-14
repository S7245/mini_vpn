# Knife14h10d16 Local Uplink Window Service Local Gate Results

Date: 2026-07-13
Base: `93abd7c88ee87f97676e64e5ce59db683b494b84`
Verdict: **LOCAL PASS; VPS P1 ELIGIBLE**

## Implemented Seam

The exact locked `smoltcp 0.10.0` source is vendored with one optional TCP
receive-window limit. Physical RX storage remains the frozen `1,048,576B`.
H10d16 listener sockets install a `368,640B` flow-control limit before
`listen`; default and non-H10 sockets retain upstream behavior.

For queued receive bytes `q`, the limited socket now uses:

```text
max_receive_extent = min(physical_capacity, configured_limit)
advertised_free    = max_receive_extent - q
acceptance_end     = application_consumed_seq + max_receive_extent
```

The limit therefore governs SYN advertisement, scaled established ACKs, and
segment acceptability. Reset/relisten preserves it. No queue, task, timer,
wake source, storage size, or environment knob was added.

## Focused TDD

The first protocol RED failed to compile because upstream smoltcp had no
receive-window-limit API. The initial minimal implementation then produced a
second semantic RED: after queuing `100,000B`, it still advertised `368,640B`
instead of `268,640B`. Capping physical free space additively would have moved
the TCP right edge forward before application consumption.

The GREEN subtracts unconsumed queue bytes. A combined test now queues
`100,000B`, verifies the scaled `268,640B` advertisement, sends a segment at
the fixed `application_consumed_seq + 368,640` edge, and proves that the
segment is rejected without increasing the queue.

## Exact 32 MiB Gate

The final real-Quinn forward tracer used the formal VPS-equivalent H10d16
profile: `1 MiB` smoltcp RX/TX storage, the new independent receive-window
limit, MTU `1200`, bounded `500`-packet modeled kernel and pump queues, the
frozen `64 KiB` application chunk, and EndpointWindowV1.

```text
receiver throughput:              302.246 Mbit/s
payload elapsed:                  888.134958 ms
sent / received:                  33,554,432B / 33,554,432B
pattern errors:                   0
ACK / clean EOF:                  yes / yes
modeled kernel-ring high water:   15 / 500 packets
pump FIFO high water:             15 / 500 packets
pump full waits / read errors:    0 / 0
uplink receive-queue high water:  39,440B / 368,640B
TCP packets:                      28,933
TCP batches/polls/flushes:        13,798 / 13,798 / 13,798
avoided per-packet services:      15,135
D16 terminal ownership/tail:      zero / clean
```

The strict local `>170 Mbit/s` discriminator passes with zero capacity
pressure. The endpoint wire-byte theorem remains separate: the local TCP
credit is payload bytes, and its queue-safety conversion is
`ceil(368,640 / 1,160) = 318 < 500` MTU-sized packets.

## Regression Gates

- vendored smoltcp enabled-feature suite: `290/290` unit and `3/3` doc tests
  (`1` expected ignored doc);
- mini_vpn default library: `630/630` nonignored (`3` expected ignored), plus
  `2/2` binary tests;
- harness library: `641/641` nonignored (`3` expected ignored), plus `2/2`
  binary tests;
- integration: `10/10`, with `4` expected ignored measurements;
- explicit TCP concurrency: `64/64`, `256/256`, `1024/1024`;
- UDP sweep: `500/500` exact at `1000B`, `1400B`, fragmented `4000B`, and
  fragmented `8000B`;
- vendored quinn-proto: `309/309` unit and `3/3` doc tests;
- vendored Quinn with the explicit local proto patch: `29/29` nonignored,
  `3` expected ignored, and `1/1` doc;
- all-target harness check, root fmt, both runner syntax/self-tests, focused
  clippy with known pre-existing Rust 1.95 lint allowances, and diff checks:
  PASS.

The newly path-vendored smoltcp emits inherited upstream warnings under the
minimal feature set; no mini_vpn warning or new clippy finding remains.

## Code Review

Review covered receive-window scaling, the fixed right edge, zero-window,
SYN/established behavior, reset/relisten, no-scaling peers, default-path
compatibility, H10 reachability, permit-before-dequeue, lifecycle, hot-path
cost, observability, vendored provenance, secrets, and missing tests.

Two review findings were fixed before this record:

1. The initial additive free-window formula was replaced with the invariant
   `limit - unconsumed_queue`, and a fixed-right-edge test was added.
2. The architecture document now distinguishes EndpointWindowV1 wire bytes
   from local TCP payload credit and bases local queue safety on packet count.

The vendored tree differs from the exact crates.io source only in
`src/socket/tcp.rs` plus the local `PATCHES.md`. No unresolved P0/P1 remains.
No macOS TUN test ran.

## VPS Eligibility

One frozen target-only forward P1 is eligible after the implementation commit.
It must require receiver `>170 Mbit/s`, zero TUN drops, pump high-water below
`500/500`, zero pump full waits, QUIC loss `<=16 MiB`, exact endpoint
conservation, the startup `receive_window_limit=368640B` fingerprint, and
clean lifecycle/cleanup. Any failure closes this architecture branch without
constant tuning.
