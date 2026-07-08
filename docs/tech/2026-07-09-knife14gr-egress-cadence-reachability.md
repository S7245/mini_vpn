# 2026-07-09 Knife14gr Egress Cadence Reachability Gate

## Goal

Apply the project `Performance Architecture Gate` before any more throughput
code or VPS runs.

This document answers whether the current B7-era code path has a code-level
sufficient route to `>30 Mbit/s` or `100+ Mbit/s`, and what the next
architecture slice must prove before implementation.

## Target Capacity Math

Decimal throughput targets:

- `30 Mbit/s` = `3.75 MB/s`.
- `100 Mbit/s` = `12.5 MB/s`.
- `173 Mbit/s` = `21.625 MB/s`.

At safe1200 MTU, TCP payload is roughly `1160B` per packet after IPv4/TCP
headers:

- `30 Mbit/s` needs about `3233` payload packets/sec.
- `100 Mbit/s` needs about `10776` payload packets/sec.
- `173 Mbit/s` needs about `18642` payload packets/sec.

Current configured local capacities in the B7 run:

- `DEFAULT_DOWNLINK_FLUSH_MAX_BYTES = 262144B`.
- timer tick is `5ms`, so a purely ideal timer-only path with one full
  `262144B` flush per tick has a theoretical byte budget of about
  `52.4 MB/s` (`419 Mbit/s`).
- B7 watermarks:
  - `high=327272B`, `low=81818B`
  - `tx_queue_flush_threshold=449999B`
  - `tx_queue_credit_spend_threshold=557386B`
  - `tx_queue_pause_threshold=572726B`

B7 reality:

- `remote_to_global_rx_bytes=64510664B` over `30s`, about `2.15 MB/s`.
- `send_slice_accepted=64510664B`, so bytes entered smoltcp at the same rate.
- `flush_attempts=8137`, about `271 attempts/sec`.
- Average accepted bytes per flush attempt were only about `7930B`.
- The run therefore did not fail because the nominal `262144B` flush cap is too
  low. It failed because most flush opportunities did not produce sustained
  large local egress progress.

The code has enough nominal batch capacity, but the current scheduler does not
prove sustained local egress cadence.

## End-To-End Hot Path Inventory

Remote read service:

1. `spawn_remote_relay` / `run_relay` read TUIC Connect stream bytes.
2. `RelayEvent::Data` is sent through `global_tx`.
3. The main loop receives it through `global_rx.recv`.

Buffered downlink/write admission:

4. `handle_remote_payload` appends bytes to `SocketCtx.downlink_pending`.
5. `flush_downlink` attempts one bounded write into the smoltcp `TcpSocket`
   send queue.
6. `handle_remote_payload` may call `iface.poll` plus `device.flush_tx` once if
   `DownlinkEgressPacer::allow_remote_payload_flush` allows immediate egress.
7. The handle is inserted into `dirty` when pending remains or bytes were
   accepted.

Old local egress continuations:

8. `timer.tick` fires every `5ms`, calls `iface.poll` and `device.flush_tx`,
   then optionally drains TUN RX ACK/window packets and calls
   `process_dirty_relay`.
9. `process_dirty_relay` calls `process_listener_activity`.
10. `process_listener_activity` calls `flush_downlink` once for each dirty
    handle, then handles uplink/local-close work.
11. `process_ready_tun_rx_packet` handles inbound TUN packets, calls
    `iface.poll`, calls `device.flush_tx`, then calls `process_dirty_relay`.
12. `drain_ready_tun_rx` repeatedly calls `try_recv_rx` and
    `process_ready_tun_rx_packet` for a bounded packet budget.

Device boundary:

13. `VirtualTunDevice::receive` consumes at most the current single
    `rx_buffer`.
14. `VirtualTunDevice::try_recv_rx` reads one nonblocking TUN packet into that
    single `rx_buffer`.
15. `VirtualTunDevice::flush_tx` writes every queued packet to the OS TUN
    interface.

The main loop owns smoltcp and the TUN device, so the next architecture should
stay in the main loop unless a larger event-loop ownership split is designed.

## Necessary vs Sufficient

B1-B7 was necessary but not sufficient.

It fixed the remote read-credit side:

- `remote_read_service_len_min=65536`
- `remote_batch_limit_bytes_min=524288`
- `read_credit_pause_updates=0`

It did not replace the local egress writer/scheduler:

- `handle_remote_payload` still does at most one local flush attempt per remote
  payload event.
- `process_listener_activity` still does one `flush_downlink` per dirty handle
  per dirty pass.
- dirty progression is still driven by timer/inbound-TUN events, not by an
  explicit "continue until local egress stops making progress" service lane.
- ACK/window packet intake is bounded by opportunistic `drain_ready_tun_rx`
  budgets and single-packet `try_recv_rx` reads.

Therefore the current code cannot be described as sufficient for `>30 Mbit/s`
or `100+ Mbit/s`.

## Old-Path Audit

Still active old paths:

- `DownlinkEgressPacer` only decides whether the current remote-payload event
  gets an immediate `iface.poll`/`flush_tx`. It is not a continuous egress
  pump.
- The `5ms` timer remains the main background progress mechanism for
  `iface.poll`, `flush_tx`, and dirty downlink work.
- `flush_downlink` remains coupled to smoltcp `send_queue`, drain credit, and
  hard-edge guards. These are needed for safety, but they do not create
  continuous service by themselves.
- TUN RX ACK/window service is split across `device.wait_for_rx`,
  pre/post-remote-payload drains, relay-gap drains, deferred drains, and timer
  active-flow drains. There is no single state machine that says: "while this
  reverse flow is active and under hard limits, alternate ACK intake, smoltcp
  poll, pending flush, and TUN flush until the egress target is reached or
  bounded progress stops."
- Diagnostics aggregate `tun_flush_tx_calls`, but they do not report
  `tx_queue` packet/byte counts per `flush_tx`, TUN RX ACK packet rates, or
  egress-service cycles.

This is not yet a full local data-plane architecture replacement.

## Failure Discriminators For The Next Slice

The next implementation must be able to distinguish these cases:

- Remote read service bottleneck:
  `remote_read_service_len_min`, `remote_batch_limit_bytes_min`,
  `read_credit_pause_updates`, and `global_rx_pressure_events`.
- Local buffer bottleneck:
  `downlink_pending`, `pending_high`, per-flow/global buffered watermarks.
- smoltcp write-admission bottleneck:
  `send_capacity`, `send_queue`, `send_slice_accepted`,
  `headroom_limited`, `budget_limited`, `send_slice_zero/errors`.
- TUN TX bottleneck:
  per-cycle `tx_queue` packet/byte counts, `flush_tx` calls/failures, and
  Linux `tx_dropped_delta`.
- TUN RX ACK/window bottleneck:
  packets drained from TUN RX per egress cycle, `would_block`, budget
  exhaustion, and whether ACK intake lowers smoltcp `send_queue`.
- Scheduler/cadence bottleneck:
  active egress service cycles, progress bytes per cycle, no-progress exits,
  cycle budget exits, and max gap between useful local egress progress.
- Lifecycle/close-tail bottleneck:
  `pending_at_close`, `terminal_pending_reap`, `terminal_late_remote_payload`,
  `egress_at_close`.

Without these discriminators, another VPS run can report a low Mbps number but
still fail to say which local egress component is responsible.

## Candidate Sufficient Architecture For B8

The next code slice should introduce an explicit main-loop local egress service
lane, not another read-credit or self-wake tweak.

Shape:

```text
active reverse/downlink work
  -> service_local_egress_until(...)
      loop within bounded cycle/time/byte budgets:
        1. nonblocking drain a bounded batch of TUN RX packets
        2. iface.poll(...)
        3. flush dirty downlink pending into smoltcp
        4. iface.poll(...)
        5. flush_tx(...)
        6. record progress; continue only while useful progress exists
```

This stays single-owner with smoltcp/TUN in the current main loop. It is not a
new thread. It is a structured service lane that replaces the current scattered
timer/deferred drain behavior for active reverse flows.

Minimum code-level capacity target:

- For the first `>30 Mbit/s` gate, prove at least `4 MB/s` local egress service
  capacity in harness terms.
- For the `100+ Mbit/s` path, the service lane should be able to sustain at
  least `13 MB/s` in harness terms.
- A practical code target is `>=128KiB` of accepted/egressed progress per
  `5ms` active service window, which is about `25.6 MB/s` (`204 Mbit/s`) before
  real OS/QUIC overhead. This is not a VPS promise; it is the code-level
  capacity required before a VPS run is worth doing.

Hard bounds:

- Keep per-flow and global buffered pending hard watermarks.
- Keep terminal no-recv and close-tail guards.
- Keep TUN/drop feedback as a hard pause.
- Keep per-cycle packet/time/byte budgets so one flow cannot starve the whole
  loop.
- Do not reopen broad QUIC windows, MTU/PLPMTUD, VPS tuning, stale pool, or
  iperf3 branches unless new counters contradict B7.

## TDD Plan Before VPS

Add deterministic tests around pure helpers and a harness-backed service lane:

1. `egress_service_repeats_while_send_queue_drains`
   - Given pending downlink and simulated ACK intake that lowers send_queue,
     the service lane performs multiple flush cycles in one active window.
2. `egress_service_stops_on_no_progress`
   - If `flush_downlink`, TUN RX drain, and `flush_tx` make no progress, the
     loop exits without spinning.
3. `egress_service_respects_hard_watermarks`
   - Per-flow/global hard pending, terminal no-recv, or TUN drop feedback stops
     remote reads and local service escalation.
4. `egress_service_fairness_limits_one_flow`
   - A single dirty reverse flow cannot consume unbounded cycles when other
     handles are dirty.
5. `egress_service_capacity_floor`
   - In a deterministic harness with available ACK/window progress, the service
     lane can move at least the configured per-window byte target, e.g.
     `128KiB` per active window.
6. `egress_service_diag_reports_progress_and_stop_reason`
   - Diagnostics include cycles, bytes accepted, TUN RX packets, TUN TX packets
     or bytes, no-progress exits, hard-limit exits, and budget exits.

Only after these tests pass should a focused VPS gate run.

## Acceptance Plan

First VPS gate remains the same B7 shape:

- safe1200 reverse-first P1
- `DURATION=30`
- `PARALLEL_SET=1`
- `MINI_VPN_BUFFERED_DOWNLINK=1`
- direct baseline healthy
- receiver `>30 Mbit/s`

Pass conditions:

- receiver `>30 Mbit/s`;
- no TUN drops;
- no send errors;
- close-tail clean;
- QUIC loss/blocking not primary;
- remote read service still useful;
- new egress-service diagnostics show continuous local progress rather than
  second-scale burst/idle gaps.

Fail-stop conditions:

- If remote read service stays useful but egress service reports no-progress or
  budget exits with low bytes, stop and fix the service lane before any VPS
  repeat.
- If egress service moves bytes continuously in harness but VPS stays below
  `30 Mbit/s`, compare TUN RX/TX counters before modifying the controller.
- If `>30 Mbit/s` does not pass, do not discuss `100+ Mbit/s` as reachable by
  this slice.

## Gate Conclusion

Current B7-era code does **not** have a code-level sufficient path to
`>30 Mbit/s` or `100+ Mbit/s`. It has enough nominal batch capacity, but the
local egress writer/scheduler is still event/timer driven and does not prove
continuous drain.

The next implementable architecture slice is an explicit bounded local egress
service lane in the main loop. That slice is plausibly sufficient for the next
`>30 Mbit/s` gate only if its tests prove continuous ACK/TUN/smoltcp drain and
per-window byte capacity before VPS acceptance.

Do not implement more read-credit floor, self-wake, VPS, MTU, stale-pool, or
broad QUIC-window changes as the next step.
