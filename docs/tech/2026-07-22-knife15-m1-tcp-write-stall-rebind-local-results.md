# Knife15 M1 TCP write-stall Endpoint rebind local results

Date: 2026-07-22

## Result

The exact-source `b33a3f6` HK bundle
`/tmp/mini_vpn_knife15_macos_20260722_075828.tar.gz` (SHA-256
`f8b5b2ea...`) is a real M1 failure, not an operator, Clash, route, TUN,
cleanup, or partial-tail observer failure. The user followed the reviewed
sequence; fresh baseline/direct, start/smoke, and cycle 1 passed.

Cycle 2 forward failed at the complete Target receiver interval
`151.001053s -> 152.001047s`, which carried zero bytes for `0.999994s`. The
sender stopped for most of `149s -> 167s`; buffered Target delivery hid all
but that one complete zero interval. Exact phase bytes were ultimately
delivered, so this is a continuity failure rather than truncation.

The selected connection added `356` lost packets, `480,190B` lost bytes, and
`112` congestion events in the 30-second sample spanning the event. Its cwnd
fell to `208,427B`; the matching D16 relay reported
`writer_wait_max_us=11,279,769`. At the offered `22,042,647 bit/s`, the frozen
`32 MiB` QUIC send window holds about `12.177s` of application data, matching
the observed write wait. ACK/control receive traffic continued, so the old
Endpoint-wide TX-without-RX discriminator never armed and no rebind occurred.

TUN and pump service, D16 ownership, Endpoint conservation, routes, physical
controls, resource envelopes, and cleanup remained healthy. M1 reverse-UDP
loss stayed below the independent `3%` SLO. This selects a missing business-
stream write-pressure discriminator.

## Repair

A TUIC-local `TcpWritePressure` module now owns one lock-free episode per TCP
pool connection while any relay writer is continuously blocked in
`poll_write`. Ready, error, cancellation, and drop release writer ownership;
the last release ends the episode. Generic and native/D16 TUIC writers use the
same adapter.

The existing `250ms` Endpoint recovery monitor samples these episodes. A
continuous episode that reaches the existing
`clamp(8 * max_rtt, 2s, 7s)` bound requests the already implemented socket
rebind even when unrelated RX advances. The rebind covers every write episode
currently present, so another pool member cannot cause an immediate duplicate.
A cleared/new episode may trigger a later recovery. The old no-RX trigger and
the all-live-connection authenticated current-generation recovery contract
remain active.

Rebind logs retain their script-compatible prefix and now add
`trigger=no_rx|tcp_write_stall`, `write_conn`, and `write_episode`. The repair
adds no payload queue, byte copy, retry task, or ownership transfer. A slow
peer can cause one observable rebind in a continuous write-pressure episode,
but cannot create a rebind loop or move already accepted TCP bytes to another
connection.

No frozen D16, MTU, UDP payload, pool, QUIC window, chunk, Cubic, GSO,
self-wake, Endpoint pacing, recovery, workload, or SLO value changed.

## TDD and review

The focused tests prove:

- RX progress cannot hide an eligible continuous business-stream write stall;
- one rebind covers every currently pending pool episode;
- the same episode cannot loop after recovery, while a cleared/new episode can
  trigger a new generation;
- multiple writers share one episode until the last owner clears;
- a real `AsyncWrite::Pending -> Ready` edge arms and releases pressure;
- existing no-RX, connection replacement, workload, and set-wise recovery
  behavior remains green.

Code review checked atomic publication/order, late release, episode rollover,
pool tracker alignment, adapter cancellation/drop, generic/native reachability,
rebind de-duplication, old-socket recovery, hot-path panic, log compatibility,
and TUN/UDP/D16 regression risk. The initial hot-path tracker `expect` was
replaced with propagated failure, and the state name was generalized from the
old no-RX-only meaning. No unresolved P0/P1 remains.

## Final local gates

```text
focused Endpoint recovery       6/6
focused TCP write pressure      2/2
TUIC unit tests                 100/100
root all-targets                650 passed / 3 ignored; main 2/2
32 MiB EndpointWindowV1         240.585 Mbit/s; final 61,440/0/0B
vendored Quinn                  36 passed / 3 ignored; doc 1/1
Quinn integration               1 expected ignored
vendored quinn-proto            309/309; doc 3/3
cargo build --release           PASS
cargo clippy --all-targets      PASS (established warnings only)
Knife15 runner self-test        PASS
Knife15 wrapper self-test       PASS
Knife14 three shell self-tests  PASS
shell syntax / root fmt / diff  PASS
changed-content secret scan     PASS
```

The runner self-test's printed one-second timeout error is its expected
negative fixture; the command exited successfully. The standalone Quinn gate
used the required absolute local `quinn-proto` patch.

## Accepted stop position

This repair is locally sufficient for the exact missing-discriminator class,
but the failed partial run is not an eight-hour M1 acceptance. Take one fresh
user-operated HK M1 from the pushed repair with a rebuilt release and fresh
baseline/direct artifacts. Disable Clash-TUN and every other VPN/TUN before
baseline and keep them disabled through Knife15 `stop`. On failure preserve
`status -> snapshot -> stop`. Do not tune frozen values or waive the TCP/UDP
SLIs. M2 and M3 remain blocked until a complete M1 passes.
