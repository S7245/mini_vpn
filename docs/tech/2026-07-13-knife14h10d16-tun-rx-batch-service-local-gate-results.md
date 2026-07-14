# Knife14h10d16 TUN RX Batch Relay Service Local Gate Results

Date: 2026-07-13
Status: **PASS locally; VPS acceptance pending**

## Outcome

The H10d16 local TUN ingress path now preserves packet-by-packet protocol
processing while servicing dirty TCP relays once per bounded ready batch. The
default non-H10 path remains on its original one-packet Adapter. No D16,
endpoint pacing, MTU, pool, QUIC window, application chunk, Cubic, GSO,
self-wake, queue, or drain constant changed.

The logical path is:

```text
ready TUN packet(s)
  -> ingest each packet exactly once
  -> classify DNS/UDP/TCP and poll/flush smoltcp per TCP packet
  -> one ControlOnly dirty-relay pass for a non-empty TCP batch
  -> existing bounded egress-actor phase/admission pass
```

## TDD And Reachability

- RED: eight ready TCP packets caused eight `process_dirty_relay` entries.
- GREEN: the same eight packets caused one batch relay entry.
- UDP-only: four packets caused zero TCP relay entries.
- mixed TCP/UDP: two TCP plus two UDP packets caused one relay entry.
- the direct H10 ready branch initially produced zero batch counters in the
  full path; routing its already-ready first packet through the same bounded
  drain made the batch architecture reachable without changing the default
  path.
- a single-slot RED proved `wait_for_rx` could overwrite a packet prefetched
  by the drain budget probe. The production and harness devices now preserve
  an already-populated RX slot; the exact-order test is GREEN.

## Exact Forward Capacity

The real localhost gate used:

- exact `32 MiB` patterned payload and `64 KiB` application writes;
- real Quinn bidirectional stream and the production native D16 writer;
- production event loop and smoltcp TCP path;
- `1200` MTU, pool `2`, a bounded `500`-packet TUN ingress ring, and `1 MiB`
  generator socket buffers;
- no D3 self-wake override and no pacing/queue/constant change.

Result:

```text
receiver:                         319.455 Mbit/s
delivered:                        33,554,432B exact
pattern errors:                   0
modeled TUN ring drops:           0
ring high water:                  29 / 500 packets
TCP packets:                      28,934
non-empty TCP batches:            4,093
batch dirty-relay passes:         4,093
avoided per-packet relay passes:  24,841
EOF / D16 tail:                   clean / 0B
owned / pending / inflight:       0B / 0B / 0B
terminal drop / late payload:     0B / 0B
```

The strict `>170 Mbit/s` architecture discriminator passes. The existing
reverse `64 MiB` bounded-ring path, full real-Quinn D16/TUN reverse path,
suppressed-wait bidirectional control test, and timer-free ACK actor wake all
remain green.

## Full Local Gates

- mini_vpn root library: `625/625` nonignored, `3` expected ignored;
- harness library: `636/636` nonignored, `3` expected ignored;
- constant integration suite: `10/10`, `4` expected ignored;
- explicit concurrency: `64/64`, `256/256`, `1024/1024`;
- UDP sweep: `500/500` exact at `1000B`, `1400B`, fragmented `4000B`, and
  fragmented `8000B`;
- vendored quinn-proto: `309/309` unit and `3/3` doc;
- vendored Quinn: `29/29` nonignored, `3` expected ignored, and `1/1` doc;
- `cargo check --all-targets --features harness`: PASS without warnings;
- root fmt, runner syntax/self-test, and diff checks: PASS.

Real localhost throughput tests now share a test-only async guard. Before the
guard, seven independent capacity gates ran concurrently and all preserved
exact bytes/lifecycle but fell below their rate thresholds together. Each had
already passed alone; serializing only these resource discriminators made the
full suite deterministic without weakening a threshold or touching product
code.

No macOS TUN test ran.

## Code Review

Review covered packet order and exactly-once ownership, SYN-before-poll,
TCP/UDP/DNS mixed batches, dirty-set lifecycle, D16 admission and channel
backpressure, EOF and terminal cleanup, backlog transitions, no-busy-wake,
EndpointWindowV1 conservation, default-path equivalence, diagnostics, parser
compatibility, and secret exposure.

The prefetch overwrite and parallel-capacity false-negative issues were fixed
and their affected gates rerun. No unresolved P0/P1 remains.

## Next Gate

Commit the reviewed local stage, deploy an isolated source snapshot, and run
one target-only forward VPS P1 with EndpointWindowV1 and the entire accepted
Gate profile frozen. Require receiver throughput strictly above
`170 Mbit/s`, zero TUN RX/TX drops, QUIC loss no greater than `16 MiB`, exact
batch counter equality with nonzero avoided passes, endpoint conservation,
and clean pool/lifecycle state. If any TUN drop remains, classify this batch
architecture as insufficient and do not tune batch or drain constants.
