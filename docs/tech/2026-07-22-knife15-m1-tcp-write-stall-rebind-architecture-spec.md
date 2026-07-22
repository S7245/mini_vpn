# Knife15 M1 TCP write-stall Endpoint rebind architecture spec

> Historical note: the `poll_write(Pending)`-alone trigger specified here was
> falsified by bundle `...094608.tar.gz`. Its trigger semantics are superseded
> by `2026-07-22-knife15-m1-stream-ack-qualified-rebind-architecture-spec.md`.

Date: 2026-07-22

## Accepted evidence

The exact-source `b33a3f6` HK M1 bundle
`/tmp/mini_vpn_knife15_macos_20260722_075828.tar.gz` (SHA-256
`f8b5b2ea...`) was operated correctly. Fresh physical baseline, 300-second
direct, start, smoke, and M1 cycle 1 passed. M1 cycle 2 forward then failed the
strict Target receiver-continuity SLI at complete interval `151.001053s ->
152.001047s`.

This is not the old partial-tail observer class. The local sender stopped for
most of `149s -> 167s`, while the Target receiver's existing buffered data hid
all but one complete zero-byte second. The data connection's 30-second QUIC
sample spanning the event added `356` lost packets, `480,190B` lost bytes, and
`112` congestion events; cwnd ended at `208,427B`. Its D16 relay close evidence
recorded a maximum upstream writer wait of `11,279,769us`.

At the same time:

- Endpoint and connection RX continued through ACK/control traffic;
- no Endpoint rebind was requested;
- Target/Exit routes, TUN, process, resources, Endpoint conservation, and
  cleanup remained healthy;
- same-window Exit and gateway probes had zero loss, though their 30-second
  cadence cannot reject a flow/five-tuple-specific QUIC path event;
- M1 UDP loss stayed below the `3%` SLO.

The selected defect is therefore a recovery-discriminator gap: a TUIC TCP
stream can be continuously write-blocked by QUIC's unacknowledged-data send
window while unrelated or ACK RX keeps the Endpoint-wide TX-without-RX monitor
disarmed.

## Goal

Treat a continuous TUIC TCP `poll_write -> Pending` episode as independent
Endpoint recovery evidence. If it reaches the already accepted RTT-derived
stall bound, perform the existing socket rebind once for that continuous
episode and retain the old socket until every sampled live connection
authenticates on the current socket generation or drains.

This repair is intended to be sufficient for the observed M1 failure class:
a live QUIC connection whose business stream cannot accept another byte for
longer than the recovery bound even though packets continue to arrive. A real
HK M1 remains necessary to quantify final sufficiency.

## Non-goals and frozen values

This stage does not change D16, TUN MTU `1200`, UDP payload `1160`, TCP pool
`2`, QUIC receive/send windows, chunk sizes, Cubic, GSO default, self-wake,
Endpoint pacing, M1 workload, SLOs, or the recovery sample/bound constants
(`250ms`, `clamp(8 * max_rtt, 2s, 7s)`). It does not reopen bounded sender,
PacerCap64, GSO-only, or parameter tuning.

It also does not move a live TUIC Connect stream between QUIC connections.
Such a move cannot preserve transparent TCP byte ownership once bytes have
been accepted by the original QUIC send stream.

## Capacity and reachability gate

Cycle 2 offered `22,042,647 bit/s = 2,755,330.875B/s`. The frozen `32 MiB`
QUIC send window represents about `12.177s` at that application rate. The
observed `11.280s` maximum writer wait is therefore capacity-consistent with a
loss event filling the unacknowledged-data window; increasing the window would
only postpone the same failure and increase memory ownership.

The repair path has a `250ms` observation cadence and a `2s` bound at the
observed `~159ms` RTT (`8 * RTT < 2s`, so the accepted minimum applies). It can
request migration roughly nine seconds before the observed write returned.
No payload copy or queue is added. The hot path performs atomic work only when
a writer changes between ready and pending states.

Exact forward hot path:

1. `process_listener_activity` reserves bounded relay-channel capacity and
   removes local bytes from the smoltcp receive queue.
2. `run_relay_writer` coalesces at most one TCP socket buffer and calls the
   TUIC native writer.
3. The TUIC writer adapter records the first `Pending` poll and clears it on
   write completion/error/drop.
4. `start_endpoint_recovery_monitor` samples per-pool-connection write
   pressure every `250ms`.
5. `EndpointRecoveryState::observe` selects either the existing no-RX trigger
   or the new continuous-write trigger.
6. `rebind_client_endpoint_udp_socket` switches the shared Endpoint socket;
   vendored Quinn retains the prior socket according to the already accepted
   all-live-connection recovery invariant.

The old TX-without-RX detector remains active and unchanged. The new signal is
parallel evidence, not a replacement.

## Module and invariants

`TcpWritePressure` is a deep TUIC-local module. Its interface exposes only a
snapshot: whether any writer is continuously pending, the monotonically
increasing pending episode, and its duration. Quinn polling and atomics remain
inside its implementation; client TUN and relay lifecycle code do not depend
on Quinn.

Required invariants:

1. Ready/error/drop clears that writer's pending ownership.
2. A connection-level episode remains continuous while at least one writer is
   pending; a zero-pending edge ends it.
3. A new zero-to-one pending edge gets a new nonzero episode.
4. RX progress cannot cancel an eligible write-stall trigger.
5. At most one rebind is issued for each connection/continuous-write episode.
6. One rebind covers all write episodes pending at that instant, preventing
   a pool member from immediately causing a duplicate Endpoint rebind.
7. Existing post-rebind all-connection/current-generation recovery remains
   mandatory.
8. No new unbounded queue, task, retry, or byte ownership is introduced.

## Failure discriminators

- `trigger=tcp_write_stall`, connection id, episode, and pending duration on a
  rebind line prove the new branch is reachable.
- `tuic-endpoint-rebind-recovered ... connections=2` proves both sampled pool
  connections authenticated the new socket.
- A repeated rebind for the same pending episode is a repair regression.
- A future receiver-zero interval with no writer stall selects another cause;
  do not tune constants.
- A writer-stall rebind that occurs but does not recover before the same
  receiver-zero class rejects this candidate as sufficient and requires a new
  architecture stage.
- Endpoint conservation, D16 ownership, TUN errors, routes, physical controls,
  resource envelopes, UDP loss, and cleanup remain independent gates.

## Test gate

TDD proceeds vertically:

1. RED/GREEN: RX progress plus one eligible continuous write episode requests
   one `tcp_write_stall` rebind.
2. RED/GREEN: the same episode cannot request a second rebind after recovery;
   a cleared/new episode can.
3. RED/GREEN: the write-pressure module starts, shares, clears, and advances
   episodes without leaking pending ownership on drop.
4. Regression: the existing no-RX trigger and all-connection recovery tests
   remain green.
5. Full Rust, release, Clippy, shell, fmt/diff, secret, code-review, and
   32 MiB capacity gates must pass before another user-run M1.
