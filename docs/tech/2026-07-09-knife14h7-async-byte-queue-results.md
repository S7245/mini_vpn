# Knife14h7 Async Byte Queue Results

Date: 2026-07-09

## Stage

This stage executes Stage B0 from the H5/H6 continuous-pump plan: convert the
pure local byte-queue model into a real async Rust primitive that can be used by
a future product relay reader.

No VPS run was performed in this stage. The queue is not wired into
`run_relay_reader`, `RelayEvent::Data`, `handle_remote_payload`, or
`flush_downlink` yet, so a VPS run would still exercise the old product path and
would not falsify the new design.

## Code Added

- `src/tcp_downlink_pump.rs`
  - `AsyncByteBoundedFlowQueue`
  - `AsyncByteQueueWait`
  - async `push`, `try_push`, `pop_up_to`, `recv_up_to`, `wait_read_len`, and
    `close`

The new async queue uses `tokio::sync::Mutex` and `Notify` to make byte
capacity, not channel message count, the explicit producer wait edge.

## Local Proof

The new tests verify these invariants:

- A producer waits while queued bytes equal capacity.
- Draining bytes wakes a waiting producer.
- `wait_read_len(max)` blocks while capacity is zero and returns the current
  available read size after drain.
- `recv_up_to(max)` waits for producer data and releases capacity when bytes
  are popped.
- `close()` wakes waiting consumers and capacity waiters without dropping data
  that was already received before close.
- `close()` also releases a producer blocked on a full queue with an explicit
  `Closed` error, instead of leaving the task suspended.

This is the Rust-specific prerequisite for the sing-box-like contract:

```text
QUIC stream read task -> byte-bounded per-flow queue -> smoltcp-owner drain
```

## Gates

Passed:

```text
cargo test -q tcp_downlink_pump
cargo fmt --check
cargo check -q
cargo test -q --lib
```

`cargo test -q --lib` passed with `446` tests.

## What This Proves

The local async primitive can express the intended continuous-reader invariant:
remote reads can be armed based on byte capacity, and they pause only at an
explicit full-queue edge or lifecycle close.

The important API is:

```text
wait_read_len(max_read_len) -> available read size
push(bytes) -> waits for byte capacity
recv_up_to(limit) / pop_up_to(limit) -> releases byte capacity
```

## What This Does Not Prove

This stage does not prove mini_vpn throughput above `30 Mbit/s` or
`100 Mbit/s`, because the product relay path is still unchanged:

- `run_relay_reader` still derives read length from `RelayReadCredit`.
- `RelayEvent::Data` still carries only `Vec<u8>` and stream diagnostics.
- `handle_remote_payload` still appends bytes directly to
  `SocketCtx.downlink_pending`.
- `flush_downlink` does not yet release any queue permit or byte-credit token.

Therefore this stage is a necessary seam, not a sufficient product fix.

## Next Falsifiable Step

Stage B1 should wire a feature-gated product path around this contract:

1. Add an opt-in relay engine for the continuous pump.
2. Keep remote reads independent from normal local headroom/debt read credit.
3. Hold byte capacity until the smoltcp owner has accepted or retained the
   payload under explicit pending accounting.
4. Add a local product-path test proving ready remote bytes are read while
   ordinary `RelayReadCredit` is paused, and only stop at byte-queue full.

Acceptance for B1:

- Local test shows a continuous-pump engine reads/stages remote bytes despite a
  normal local-admission pause.
- Test also shows it stops at the explicit byte capacity edge.
- Existing legacy/thin relay tests still pass.

Only after B1 passes should a small VPS smoke run be useful. The first VPS
threshold remains `>30 Mbit/s` reverse-first, with the key metric being a
material drop in active-window `data_read_gap_max_ms` and
`data_pending_gap_max_ms`; if those gaps remain multi-second while queue-full
is not observed, the continuous-pump hypothesis is falsified at the product
path level.
