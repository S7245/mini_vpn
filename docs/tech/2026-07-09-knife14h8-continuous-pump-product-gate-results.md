# Knife14h8 Continuous Pump Product Gate Results

Date: 2026-07-09

## Stage

This stage executes B1 from the continuous-pump plan: wire a feature-gated
product relay engine that uses the H7 async byte queue.

The new path is opt-in only:

```text
MINI_VPN_CONTINUOUS_TCP_RELAY=1
```

If both `MINI_VPN_CONTINUOUS_TCP_RELAY=1` and
`MINI_VPN_THIN_TCP_RELAY=1` are set, the continuous pump wins. The default
engine remains `legacy`.

## Code Path

The feature-gated reverse/downlink path is now:

```text
run_relay_with_engine(ContinuousPump)
  -> run_relay_continuous_reader
  -> AsyncByteBoundedFlowQueue
  -> run_continuous_relay_dispatcher
  -> RelayEvent::Data
  -> existing handle_remote_payload / downlink_pending / flush_downlink path
```

The important B1 behavior is that the continuous reader does not use normal
local-admission `RelayReadCredit` as its remote-read clock. It reads while the
per-flow byte queue has capacity, and pauses at the explicit queue-full edge.

## Local Proof

New product-path tests:

- `continuous_relay_reader_ignores_read_credit_pause_until_byte_queue_full`
  proves a paused `RelayReadCredit` does not stop ready remote bytes from
  reaching `RelayEvent::Data` under the continuous engine.
- `continuous_relay_reader_stops_only_at_byte_queue_full_edge` proves the
  reader stops after filling a small byte queue, then resumes when the queue is
  drained.
- `tcp_relay_engine_selector_keeps_continuous_opt_in_and_priority` locks the
  engine selection rule.
- `async_byte_queue_close_allows_draining_already_queued_bytes` locks the
  close/drain contract: closing the queue stops new producer work but still lets
  the dispatcher drain bytes that were already accepted.

New observability:

- Relay live/close diagnostics now include:
  - `continuous_queue_wait_max_us`
  - `continuous_queue_wait_events`
- The reader logs `tcp-continuous-queue-wait` when a byte-capacity wait crosses
  the diagnostic threshold.

The US-client VPS suite now also carries the opt-in through its own defaults,
help, self-test, result header, and explicit `sudo -E env` launcher:

```text
MINI_VPN_CONTINUOUS_TCP_RELAY=0
```

This prevents a smoke run from silently losing the experimental engine at the
privilege boundary.

## Gates

Passed:

```text
cargo test -q continuous_relay_reader
cargo test -q tcp_relay_engine_selector
cargo test -q relay_task_diag
cargo test -q relay_close_diag
bash -n scripts/knife14b-usclient-tunnel-suite.sh
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
cargo fmt --check
cargo check -q
cargo test -q --lib
git diff --check
```

`cargo test -q --lib` passed with `450` tests.

## What This Proves

B1 proves the product relay can be switched to a Rust-native continuous reader
contract at the relay-task level:

```text
remote read waits on byte-queue capacity, not ordinary local read credit
```

This directly targets the H4/HZ failure shape where normal local/QUIC counters
were clean but remote read cadence showed multi-second active-window gaps.

## What This Does Not Prove

B1 is still not the full final architecture:

- The byte queue releases capacity when the dispatcher pops bytes into
  `RelayEvent::Data`.
- Capacity is not yet held until `SocketCtx.downlink_pending` bytes are accepted
  by smoltcp or drained by `flush_downlink`.
- The existing `handle_remote_payload`, `downlink_pending`, local egress
  service, and TUN flush path are unchanged.

Therefore B1 can falsify or support the "remote read was clocked by local
read-credit" hypothesis, but it does not yet prove the complete
`QUIC read pump -> byte queue -> smoltcp-owner drain` contract.

## VPS Smoke Gate

A small VPS smoke run is now useful, but should be limited to one reverse-first
P1 run with the continuous engine enabled.

Required acceptance signals:

- Startup/log evidence includes `engine=continuous_pump` or the startup line for
  `MINI_VPN_CONTINUOUS_TCP_RELAY=1`.
- First throughput gate: reverse-first P1 receiver exceeds `30 Mbit/s`.
- Main diagnostic improvement: active-window `data_read_gap_max_ms` and
  `data_pending_gap_max_ms` materially drop from the H4 band
  (`3685ms` / `3006ms`).
- `continuous_queue_wait_events` stays low or explains any remaining remote
  read gap as explicit byte-queue-full backpressure.

Failure interpretation:

- If throughput remains in the `15-25 Mbit/s` band and read/pending gaps remain
  multi-second while `continuous_queue_wait_events=0`, then the hypothesis that
  ordinary local read-credit clocking is the main bottleneck is falsified.
- If gaps move to `continuous_queue_wait_events>0`, then B1 found a real
  downstream byte-queue pressure edge and the next stage must extend capacity
  lifetime through `downlink_pending`/`flush_downlink` instead of changing chunk
  size.
- If throughput clears `30 Mbit/s` but not `100 Mbit/s`, the next stage should
  complete the permit-to-pending backpressure lifecycle before broad tuning.

Do not change VPS socket buffers, MTU/PLPMTUD, iperf3, stale pool behavior, or
broad QUIC windows for this smoke gate.

## VPS Smoke Attempt

Attempted on `.27` after syncing the minimal H8 file set needed for a remote
build:

- `src/client_tun.rs`
- `src/tcp_downlink_pump.rs`
- `src/lib.rs`
- `src/tcp_stream_service.rs`
- `src/device.rs`
- `scripts/knife14b-usclient-tunnel-suite.sh`

Remote preflight gates passed:

```text
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
cargo check -q
cargo test -q continuous_relay_reader
cargo test -q tcp_relay_engine_selector
cargo test -q async_byte_queue_close_allows_draining_already_queued_bytes
```

The actual VPS throughput smoke did not start. The suite failed at its
preflight `sudo -v` step because the SSH command was not running with an
interactive TTY:

```text
sudo 校验失败
```

Failed preflight artifact:

```text
/tmp/conn/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_173738.tar.gz
```

This artifact is not throughput evidence. It does prove the suite received
`MINI_VPN_CONTINUOUS_TCP_RELAY=1`, but it did not launch mini_vpn or run iperf.

## VPS Smoke Result

After rerunning the suite through a real TTY, the H8 reverse-first P1 smoke did
start and complete.

Artifacts:

```text
/tmp/conn/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_174648.tar.gz
/tmp/mini_vpn_knife14h8_continuous_pump/mvpn_knife14h8_continuous_pump_p1_usclient_suite_20260709_174648.tar.gz
```

Activation evidence:

```text
continuous TCP relay pump: enabled queue=4194304B
tcp-relay-engine ... engine=continuous_pump queue_bytes=4194304
```

Throughput:

```text
reverse-first P1 sender:   30.7 Mbit/s
reverse-first P1 receiver: 29.6 Mbit/s
suite status: FAILED
```

The receiver did not clear the `>30 Mbit/s` first gate, and the run is nowhere
near the `100+ Mbit/s` target.

Key discriminator:

```text
continuous_queue_wait_events=0
continuous_queue_wait_max_us=101
data_read_gap_max_ms=3823
data_pending_gap_max_ms=3408
data_poll_gap_max_ms=3406
```

This falsifies the narrow hypothesis that ordinary local read-credit pauses
were the main remaining root. The continuous pump was active, and the 4MiB
byte queue did not become the backpressure edge, yet the data stream still
showed multi-second TUIC read/pending/poll gaps.

Other important signals:

```text
global_rx_pressure=0
local_write_pressure=0
tun_rx_dropped_delta=0
tun_tx_dropped_delta=0
quic loss/congestion/blocking=0
pending_at_close=0
terminal_pending_reap=0
downlink_backpressure pause_edges=1 resume_edges=1
pressure_credit_debt_bytes=122727
headroom_limited=6
tun_flush_deferred=3
```

Compared with H4, H8 slightly improves average throughput and reaches a
borderline `30 Mbit/s` sender number, but it does not materially remove the
bursty/idle shape. H8 should remain an opt-in diagnostic/product seam, not an
accepted throughput fix.

Next design implication:

- Stop treating relay-reader local read credit as the main root.
- Do not tune chunk size, self-wake timers, broad QUIC windows, VPS buffers,
  MTU/PLPMTUD, iperf3, stale pool, or sing-box liveness for this branch.
- The next code-level design must explain why TUIC reports
  `connection_stream_frames_pending` and active data poll gaps while the
  continuous relay queue is not full and QUIC/global_rx/local-write counters
  are clean.
- The likely next seam is the coupling between `tuic.rs` ordered stream
  readiness/pending detection and `client_tun.rs` local downlink backpressure /
  `flush_downlink` / `pressure_credit_debt` transitions.
