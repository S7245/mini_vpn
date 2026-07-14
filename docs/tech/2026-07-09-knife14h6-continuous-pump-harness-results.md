# 2026-07-09 Knife14h6 Continuous Pump Harness Results

## Goal

Execute Stage A from the Knife14h5 reachability gate: add one local
tracer-bullet harness seam for the continuous reverse TCP pump contract before
changing the product relay fast path.

This stage did not run VPS acceptance and did not tune VPS services, iperf3,
MTU/PLPMTUD, stale TUIC pool handling, broad QUIC windows, ordered chunk size,
or self-wake timers.

## Code State

Files changed:

- `src/tcp_downlink_pump.rs`
- `src/lib.rs`

Main code changes:

- Added `ByteBoundedFlowQueue`, a byte-bounded per-flow queue model with:
  - explicit zero-capacity rejection;
  - empty-chunk rejection;
  - chunk-larger-than-capacity rejection;
  - queue-full rejection with available-byte accounting;
  - high-water and queued-byte snapshots.
- Added `ContinuousPumpProbe`, a local probe for:
  - read-armed events while enough queue capacity exists for the planned read;
  - remote read bytes/chunks;
  - drained bytes;
  - explicit queue-full waits;
  - max remote-read gap while queue capacity exists;
  - max queue occupancy.
- Exposed the module from `src/lib.rs` so Stage B can reuse the seam.

This module is not wired into `run_relay_reader`, `global_rx`, or
`flush_downlink` yet. It is the local feedback loop and contract surface for
the next implementation slice.

## Local Tests

Focused Stage A tests:

- `byte_bounded_flow_queue_enforces_byte_capacity`
- `continuous_pump_model_sustains_100mbit_for_30s_without_capacity_read_gaps`
- `continuous_pump_absorbs_short_drain_stall_without_remote_read_gap`
- `continuous_pump_pauses_only_at_explicit_queue_full_edge`

The main tracer-bullet test simulates `30s` of `64KiB` reads every `5ms`,
which is about `104.9 Mbit/s`:

```text
64 KiB / 5ms = 13.1072 MB/s = 104.8576 Mbit/s
30s total = 393216000 bytes
```

The drain-stall test skips local drain for `200ms` while a `4MiB` queue still
has capacity. Remote reads continue at the `5ms` cadence, the queue absorbs
more than `2MiB`, and the backlog drains afterward.

The queue-full test uses a tiny `128KiB` queue and proves that remote reads
pause only at the explicit queue-full edge; queue-full pauses are counted as
bounded backpressure, not unexplained read gaps.

## Gates Run

```text
cargo test -q tcp_downlink_pump
cargo fmt --check
cargo check -q
cargo test -q --lib
```

Results:

- `cargo test -q tcp_downlink_pump`: passed, `4` tests.
- `cargo fmt --check`: passed.
- `cargo check -q`: passed.
- `cargo test -q --lib`: passed, `441` tests.

## Acceptance Against Stage A

Stage A acceptance from Knife14h5:

- A scripted remote stream can feed data at `100 Mbit/s` equivalent for at
  least `30s` of simulated or bounded real time.
- A bounded per-flow queue model records read armed time, queue occupancy, and
  drain progress.
- The first failing assertion targets behavior, not implementation: active data
  windows must not show remote-read idle gaps above `50ms` while queue capacity
  exists.

Status:

- Passed for the local deterministic model.
- The current test is a contract/harness seam, not yet the real async QUIC
  reader path.

## Code Review Notes

No correctness findings in this Stage A slice.

Important limitation:

- This does not prove `run_relay_reader` or Quinn stream readiness is fixed.
  It proves the local contract and gives Stage B a small interface to wire into
  a feature-gated fast path.

Implementation caution for Stage B:

- The production path must keep the same invariant with real async waiters:
  if the byte queue can accept the planned read chunk, one remote read future
  must remain armed.
- If the async implementation cannot preserve this invariant through
  `run_relay_reader`, the next step is seam extraction, not another parameter
  tweak.

## Next Step

Proceed to Stage B only behind a feature/env gate:

```text
QUIC read pump task
  -> ByteBoundedFlowQueue / async equivalent
  -> smoltcp-owner main-loop drain
```

Do not run VPS before the product path has a local test proving that active
remote read gaps stay below `50ms` while byte-queue capacity exists.
