# Knife15 M2 Successor Service-Turn App-Limited Ownership Architecture Spec

Date: 2026-08-08

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac evidence:
`/tmp/mini_vpn_knife15_macos_20260808_054225.tar.gz` (SHA-256
`0e6d8ee68340acd42cc136b65305974584d713b6a92bb81701fb45a5299c9f7c`).

Paired Exit evidence:
`/tmp/mini_vpn_knife15_exit_target_observer_20260808_052126.tar.gz`
(SHA-256
`4879b79000a2d43ab0d2b0d303a5f839c5368ca1c443e758a4d08ee96af17892`).

Exact source: `f693d0d01926eba8e0b963f2538ebf2adb56b9dd`.

## Decision

Keep the existing one-current-cwnd successor service turn, its exact tagged
packet accounting, and all failure rules. Correct one missing transport
ownership rule: an ACK for a packet tagged as part of the service turn must
enter congestion control as non-application-limited even if a later empty
transmit poll changes the connection-wide `app_limited` snapshot before that
ACK arrives.

The packet tag already proves that the carrier was deliberately emitted to
fill the snapshotted congestion window. The ACK path therefore has exact
per-packet authority and does not need a new timer, byte target, round, retry,
Target connection, or application probe.

Ordinary QUIC packets retain the existing connection-wide `app_limited`
behavior. Cubic itself, the service-turn byte budget, and every mini_vpn
policy remain unchanged.

## Failure Classification

The Mac passed baseline, direct, start, smoke, full-tunnel preflight, the
300-second forward, the 300-second reverse, the 180-second reverse UDP phase,
and cleanup. The first ten-second short forward failed with one complete
Target receiver-zero interval:

```text
Mac accepted:          10,092,544B
Target received:        4,194,304B
client zero intervals:          3
Target zero intervals:          1
writer progress:        6,300,341B
writer wait max:        3,516,046us
```

The exact owning connection was conn1 generation 2. Its replacement service
turn had succeeded at `12,000/12,800/12,800/0B`, but the immediately published
path cwnd was only `13,280B`. It then remained at `13,280B` throughout the
long reverse TCP and UDP phases because receiving bulk does not grow the
client-to-Exit congestion window.

The Exit observer captured `1,569,331` packets with zero kernel drops. During
the failed short-forward data socket, Exit sent about `4.18MB` to Target and
Target ACKed every observed byte at roughly `1..3ms`. This rejects operator,
physical-link, Target, Exit-to-Target, TUN, D16 downlink, Endpoint
conservation, route, process, and cleanup branches. Supply into the Exit
socket was the limiting seam.

The previous zero-latency Quinn test observed all turn ACKs before a later
idle poll and required only `final_cwnd > initial_cwnd`. At a deterministic
164ms RTT, the unchanged implementation produced:

```text
initial_cwnd=12,000B
turn_acked=13,068B
final_cwnd=23,616B
```

Thus at least one tagged ACK lost its slow-start authority. The VPS sequence,
with delayed ACK batches and PLPMTUD activity, retained only one MTU of growth
at the install observation (`12,000 -> 13,280B`). The original one-turn
capacity assumption was therefore not implemented end to end.

## Goals

1. Preserve send-time non-application-limited ownership for every tagged
   service-turn packet until its ACK enters congestion control.
2. Make a successful full-window turn leave the successor with at least its
   initial cwnd plus all tagged ACKed bytes while still in Cubic slow start.
3. Keep exact ACK/loss/path/close/deadline failure semantics and Endpoint bulk
   accounting unchanged.
4. Add a deterministic realistic-RTT regression through the Quinn protocol
   connection interface.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool size, QUIC windows, chunk, Cubic,
  GSO default, Endpoint rate/burst, self-wake, recovery bounds, admission
  ordering, startup priority, workload, SLI, or runner timeouts.
- Do not increase the service-turn target, number of turns, retry count, or
  elapsed deadline.
- Do not change ordinary packet application-limited classification.
- Do not claim formal-M2 or general WAN throughput acceptance.

## Invariants

1. Only `SentPacket.successor_service_turn` can override a later global
   application-limited snapshot.
2. The override applies only when that exact ACK is passed to the existing
   congestion controller on the current validated path.
3. Untagged ACKs remain byte-for-byte behavior compatible.
4. Tagged loss, path change, close, and deadline behavior remain terminal and
   cannot publish success.
5. Endpoint bulk reservation, settlement, socket ownership, and the global
   conservation equation remain unchanged.
6. No synthetic business payload or Target connection is created.

## Capacity And Reachability Gate

The failed short flow began at `05:56:35.471Z`. The Exit data socket first
appeared around `05:56:35.978Z`, after the existing control/data Connect
sequence consumed about `0.5s` of the first receiver interval. With the
observed `164ms` client-to-Exit RTT, only about three send rounds fit before
the first one-second iperf receiver boundary.

Broken installed capacity:

```text
13.28 + 26.56 + 53.12 ~= 92.96KiB < 128KiB
```

Corrected service-turn capacity, using the observed exact successful turn:

```text
initial 12,000B + acknowledged 12,800B = at least 24,800B
24.8 + 49.6 + 99.2 ~= 173.6KiB > 128KiB
```

This is intended to be sufficient for the exact cold-successor first Target
interval discriminator. It is not sufficient evidence for the complete
formal-M2 schedule. The unchanged exact 32MiB Endpoint gate must still exceed
`170 Mbit/s`, finish with exact EOF, zero socket would-block, and final
ownership at or below `61,440/0/0B`.

Hot path:

```text
replacement authenticate
  -> one current-cwnd successor service turn
  -> tagged SentPacket stored in Quinn packet space
  -> delayed ACK on the same path generation
  -> Cubic::on_ack(non_app_limited for this tagged packet)
  -> existing generation CAS install
  -> existing TUIC Connect / D16 writer / Exit / Target
```

## Old-Path Audit

Service-normalized admission, forward qualification, replacement fallback,
startup priority, writer ACK-stall rebind, connection-local path reset,
read-only recovery observer, UDP-demand recovery, D16, Endpoint pacing, and
the draining predecessor lifecycle all remain active and unchanged.

## TDD And Discriminators

The focused RED uses the real Quinn `Connection` protocol pair at `82ms`
one-way latency and asserts that a successful service turn leaves
`final_cwnd >= initial_cwnd + tagged_acked_bytes`. The pre-fix result fails at
`12,000 + 13,068 > 23,616`; the minimal tagged-ACK correction must make it
GREEN.

The next Mac qualification must show one of:

- successful turn, installed cwnd at least initial plus tagged ACK bytes, and
  no successor receiver-zero interval: retain the architecture;
- successful turn with corrected installed cwnd but another receiver-zero:
  reject this architecture without tuning or repeating it;
- loss/close/path/deadline: preserve fail-closed predecessor ownership and
  classify the existing exact failure;
- any Endpoint, TUN, D16, route, cleanup, lifecycle, or untagged congestion
  regression: reject the implementation.

## Stop Rule

Stop on any new knob, larger/multiple service turn, retry, Target probe, pool
expansion, frozen-value change, ordinary-packet behavior change, local exact
Endpoint capacity at or below `170 Mbit/s`, unexpected regression, or
unresolved P0/P1. After all local gates and review pass, take exactly one
fresh paired Mac qualification; formal M2 and M3 remain blocked.
