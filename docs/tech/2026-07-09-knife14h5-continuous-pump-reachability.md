# 2026-07-09 Knife14h5 Continuous Pump Reachability Gate

## Goal

Execute the next read-only Knife14 stage after H4: freeze the current
evidence, state the capacity math, and define the architecture gate for a
Rust-native continuous reverse TCP pump before any more mini_vpn code changes.

This is a design and acceptance document, not an accepted fix. It deliberately
does not tune VPS services, iperf3, MTU/PLPMTUD, stale TUIC pool handling,
broad QUIC windows, ordered chunk size, or generic self-wake timers.

## Evidence Frozen For This Gate

The current `.27/.33/.77` topology can move `100+ Mbit/s` through the mature
TUIC path:

- `.33` socket buffers are persistently high:
  `rmem_max=16777216`, `wmem_max=16777216`,
  `rmem_default=1048576`, `wmem_default=1048576`.
- Mature sing-box client reached `185.242 Mbit/s` receiver after the socket
  buffer fix, and a later current-window sing-box repeat reached about
  `173/173 Mbit/s`.

mini_vpn is still not stable above `30 Mbit/s`:

- Knife14hz: `15.3/14.3 Mbit/s`, clean TUN/global-rx/local pressure/QUIC
  surfaces, but ordered TUIC data read/pending gaps up to `5086ms`.
- Knife14h4: ordered `read_chunk(max, true)` path active
  (`relay_mode=ordered_chunk`), but `17.1/15.7 Mbit/s`; clean TUN,
  global-rx pressure, local write pressure, pressure credit debt, headroom,
  close-tail, and QUIC loss/congestion/blocking; remaining bad signals were
  bursty iperf output plus `data_read_gap_max_ms=3685`,
  `data_pending_gap_max_ms=3006`, and `data_poll_gap_max_ms=405`.

Rejected or de-prioritized roots for the next implementation slice:

- VPS capacity, `.33` socket buffers, sing-box service health, iperf3 service
  health, MTU/PLPMTUD, stale TUIC pool slots, broad QUIC windows, QUIC
  loss/congestion/blocking, TUN drops, global-rx pressure, local write
  pressure, close-tail pending, ordered adapter swaps alone, chunk-size tuning,
  self-wake timing alone, and local egress-drain instrumentation alone.

## Capacity Math

The target is still `100+ Mbit/s`; do not lower it to `30 Mbit/s`.

```text
100 Mbit/s = 12.5 MB/s
120 Mbit/s = 15.0 MB/s
150 Mbit/s = 18.75 MB/s
180 Mbit/s = 22.5 MB/s
```

Required active-flow read cadence:

| Unit | 100 Mbit/s | 150 Mbit/s | 180 Mbit/s |
| --- | ---: | ---: | ---: |
| 64 KiB chunks/sec | 191 | 286 | 343 |
| 64 KiB cadence | 5.2ms | 3.5ms | 2.9ms |
| 128 KiB batches/sec | 95 | 143 | 172 |
| 128 KiB cadence | 10.5ms | 7.0ms | 5.8ms |

mini_vpn already has enough nominal batch size for `100+ Mbit/s`:

- `RELAY_REMOTE_READ_MIN_BATCH_BYTES = 64 KiB`
- `RELAY_REMOTE_READ_BURST_MAX_BYTES = 512 KiB`
- HZ/H4 reported 64 KiB service lengths and up to 128 KiB remote batches.

Therefore the HZ/H4 failure is not primarily "read length too small." A
multi-second active-flow data read/pending gap is hundreds of times larger than
the cadence needed for `100+ Mbit/s`; it is a service-continuity failure.

## Current mini_vpn Hot Path

Reverse/downlink path today:

```text
TUIC QUIC stream
  -> TuicUpstream::open_tcp RelayStream
  -> spawn_remote_relay
  -> run_relay_reader
  -> RelayReadCredit gate
  -> remote_reader.read()
  -> drain_ready_remote_reads
  -> global_rx mpsc
  -> main event loop
  -> SocketCtx.downlink_pending
  -> flush_downlink
  -> smoltcp TcpSocket::send_slice
  -> iface.poll
  -> VirtualTunDevice tx_queue
  -> flush_tx
  -> OS TUN
```

Important coupling points:

- `run_relay_reader` derives `remote_read_len` from `RelayReadCredit` and only
  reads when the credit is non-zero.
- `publish_relay_read_credit_for_handle` computes read credit from smoltcp
  send queue, pending downlink, headroom, controller state, and hard-pause
  state.
- `flush_downlink` feeds accepted bytes and headroom limits back into the
  downlink credit controller.

This is not equivalent to the mature sing-box path. sing-box uses a per-flow
download copy goroutine:

```text
remote net.Conn Read
  -> CopyWithIncreateBuffer
  -> local gVisor TCP net.Conn Write
```

The sing-box download goroutine is naturally backpressured by `Write`; it does
not require a main-loop predictor to authorize each remote read.

## Target Rust Architecture

The target is a Rust-native equivalent of the sing-box data-plane contract,
without violating smoltcp ownership:

```text
QUIC read pump task
  -> bounded per-flow byte queue
  -> smoltcp-owner main-loop drain
  -> iface.poll / flush_tx active service
```

Ownership rule:

- `TcpSocket` and `SocketSet` stay owned by the main loop. No task receives a
  mutable smoltcp socket.

Backpressure rule:

- If the per-flow byte queue is below its byte cap, the QUIC read pump must keep
  one remote read future armed.
- If the queue is full, the read pump awaits queue capacity.
- Local write slowness can fill that flow's queue and naturally pause that
  flow's read pump, but it must not create global starvation or multi-second
  idle windows while the queue has capacity.

Lifecycle rule:

- Remote EOF is not terminal until queued remote bytes and smoltcp pending bytes
  have been drained or explicitly accounted as terminal loss.
- A single failed relay/session must not tear down unrelated sessions.

Bounded memory rule:

- Every queue is byte-bounded, not only message-count bounded.
- The first design target should be small enough to protect concurrency but
  large enough to absorb normal RTT/scheduler jitter. A candidate envelope is
  `2-4 MiB` per active flow with a separate global byte cap, but the exact
  value belongs to the implementation stage after the local harness exists.

## Necessary vs Sufficient Classification

This gate is necessary, not sufficient, for final `100+ Mbit/s` acceptance.

It is intended to be sufficient for the next code-level design claim:

- mini_vpn has a plausible continuous service path to exceed `30 Mbit/s`; and
- if the local harness passes, the next VPS run is testing a design with a
  real `100+ Mbit/s` capacity path instead of another parameter tweak.

It is not sufficient to declare final product acceptance because real QUIC,
kernel TUN, smoltcp ACK/window behavior, RTT, and scheduler behavior still
require VPS validation.

## Old-Path Audit

The following old paths must either be bypassed on the new fast path or clearly
demoted to protection/diagnostic mode:

- Normal remote read authorization through predictive local
  pressure/headroom/debt credit.
- Remote-read progress depending on periodic probe/self-wake timers.
- Message-count-only `global_rx` pressure as the primary downlink buffer.
- Local egress progress counters as a substitute for continuous read/drain
  service.

The following paths remain required:

- smoltcp single-owner socket discipline.
- bounded queue/backpressure guards.
- close-tail accounting.
- TUN drop/flush diagnostics.
- QUIC loss/congestion/blocking diagnostics.

## Falsifiable Hypotheses

1. Continuous read-pump hypothesis:
   If mini_vpn's low reverse throughput is caused by remote read service
   discontinuity, then keeping a remote read future armed whenever the per-flow
   queue has capacity will collapse active-window read/pending gaps and move
   the VPS result above the `30 Mbit/s` first gate.

2. Local TCP/TUN drain hypothesis:
   If remote read gaps collapse but throughput remains low, then the remaining
   primary difference is smoltcp/TUN ACK-window or local drain cadence versus
   sing-box gVisor/TUN offload.

3. QUIC/source cadence hypothesis:
   If the new pump keeps a read future armed and the per-flow queue has
   capacity, but VPS still shows multi-second no-data periods with fresh QUIC
   stream pending evidence, then the next root is Quinn per-stream readiness,
   ordered stream offsets, or server/source cadence rather than mini_vpn local
   admission.

4. Queue-bound hypothesis:
   If throughput improves but memory grows without bound, tail drops appear, or
   unrelated sessions stall, then the pump is too aggressive and lacks a correct
   byte-bounded backpressure contract.

## Stage List From This Gate

### Stage A - Feedback Loop / Harness Seam

Goal: add one local tracer-bullet harness before product fast-path changes.

Acceptance:

- A scripted remote stream can feed data at `100 Mbit/s` equivalent for at
  least `30s` of simulated or bounded real time.
- A bounded per-flow queue model records read armed time, queue occupancy, and
  drain progress.
- The first failing assertion must target behavior, not implementation:
  active data windows must not show remote-read idle gaps above `50ms` while
  queue capacity exists.

### Stage B - Continuous Pump Behind A Gate

Goal: introduce a feature-gated fast path using branch by abstraction.

Acceptance:

- Old path remains available by default until the new path passes local and VPS
  gates.
- The new path uses a byte-bounded per-flow queue and explicit global byte cap.
- Remote read pauses only for queue-full, EOF, cancellation, or hard terminal
  lifecycle state.
- Local gates pass: targeted harness, full tests, clippy, fmt, release build.

### Stage C - Local 100M-Equivalent Gate

Goal: prove local service continuity before any VPS run.

Acceptance:

- `100 Mbit/s` equivalent reverse input for at least `30s`.
- `remote_read_gap_max_ms < 50`.
- Queue occupancy remains bounded and drains after remote EOF.
- No dropped bytes; total bytes read equals total bytes accepted or explicitly
  pending until drained.
- Close completes with per-flow queue `0` and terminal pending `0`.

### Stage D - VPS First Threshold

Goal: prove the new architecture escapes the historical `15-25 Mbit/s` band.

Acceptance:

- safe1200 reverse-first P1 receiver `>30 Mbit/s`.
- `data_read_gap_max_ms < 500`.
- `data_pending_gap_max_ms < 500`.
- TUN drops `0`, global-rx pressure clean, local write pressure clean or
  explicitly explained by the new queue metrics, QUIC loss/congestion/blocking
  clean, close-tail pending `0`.

### Stage E - VPS 100M Gate

Goal: prove the architecture is plausibly sufficient for the target.

Acceptance:

- At least two same-window safe1200 reverse-first P1 runs with receiver
  `>100 Mbit/s`.
- No `tx_dropped_delta`.
- No `terminal_pending_reap_bytes`.
- No data-relay `terminal_closed_no_send`.
- Active-window read/pending gaps stay materially below the H4/HZ second-scale
  failures; target `<100ms`, hard first ceiling `<250ms`.

### Stage F - Stability Expansion

Goal: move from single-flow throughput to product requirements.

Acceptance:

- Longer reverse duration.
- P2/P4 or equivalent concurrent reverse flows.
- Forward/reverse mixed TCP.
- UDP/live-streaming regression.
- Bounded memory under concurrent flows.
- One relay/session failure does not tear down unrelated sessions.

## Stop Rules

- If Stage A cannot build a correct local harness seam, stop implementation and
  extract the seam first; do not patch the current relay loop in place.
- If Stage C fails, do not run VPS; the design has not earned an integration
  test.
- If Stage D passes `30 Mbit/s` but read/pending gaps remain second-scale, treat
  the throughput as unstable evidence, not an accepted architecture.
- If read/pending gaps collapse but throughput stays low, stop remote-read work
  and switch to the smoltcp/gVisor/TUN drain hypothesis.
- If any VPS failure occurs, compare expected invariants with observed counters
  and write the next modification plan before editing code.

## Design Score

Current design score: `8/10`.

Why not `10/10` yet:

- The per-flow/global byte caps need implementation-level sizing after the
  local harness exists.
- The exact main-loop wake/drain scheduling contract still needs a tracer test.
- The H4 ordered chunk adapter still needs a final decision: revert, opt-in
  diagnostic, or internal seam for the new pump.

The design is strong enough to move to Stage A, but not strong enough to skip
the local harness or claim `100+ Mbit/s` before VPS evidence.
