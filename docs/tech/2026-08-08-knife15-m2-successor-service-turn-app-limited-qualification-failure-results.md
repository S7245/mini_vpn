# Knife15 M2 Successor Service-Turn App-Limited Qualification Failure Results

Date: 2026-08-08

Status: **FAILURE CLASSIFIED; APP-LIMITED ACK OWNERSHIP SELECTED; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260808_054225.tar.gz`.

Mac SHA-256:
`0e6d8ee68340acd42cc136b65305974584d713b6a92bb81701fb45a5299c9f7c`.

Exact source: `f693d0d01926eba8e0b963f2538ebf2adb56b9dd`.

Paired Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260808_052126.tar.gz`.

Exit SHA-256:
`4879b79000a2d43ab0d2b0d303a5f839c5368ca1c443e758a4d08ee96af17892`.

## Accepted Controls

- direct baseline: `21.003/42.391 Mbit/s`;
- 300-second direct forward: `10.497 Mbit/s`, with no complete receiver gap;
- start, smoke, IPv6, full-tunnel, real-client, route, TUN, process, and
  interface preflights: PASS;
- cycle-1 300-second forward: about `10.48 Mbit/s`, no receiver-zero interval;
- cycle-1 300-second reverse: about `21.195 Mbit/s`, no receiver-zero interval;
- cycle-1 reverse UDP: complete, `1.630043%` loss;
- cleanup: route, DNS, TUN, process, and Endpoint ownership PASS.

Endpoint maximum/final ownership was `61,440B / 61,414/0/0B`, with no
interface errors. Formal M2 was not run.

## Exact Failure

The first ten-second short forward failed its unchanged Target continuity SLI:

```text
Mac sender/accepted:       10,092,544B
Target received:            4,194,304B
Mac sender-zero intervals:           3
Target receiver-zero intervals:      1
D16 writer progress:        6,300,341B
D16 writer max wait:        3,516,046us
```

The exact owner was conn1 generation 2. Its predecessor had advanced from
zero to one PLPMTUD black hole, so replacement authenticated a successor and
ran the existing service turn. The turn reported success at
`target/sent/acked/lost = 12,000/12,800/12,800/0B` in `330ms`, but the path
snapshot published immediately after install had only `13,280B` cwnd. The
following reverse TCP and UDP phases could not grow that client-to-Exit
forward window, so the short forward began on an almost-initial successor.

Endpoint bulk grants advanced by megabytes during the short flow, D16 made
progress, and the connection later grew beyond `300KiB`; this rejects the
Endpoint window, D16 capacity, or a permanent transport outage as the cause.

## Paired Exit Evidence

The observer captured `1,569,331` packets with zero capture/kernel drops. The
failed short data socket was `172.26.0.2:47610 -> 43.130.32.77:5201`. Once it
appeared, Exit supplied bytes continuously and Target ACKed every observed
byte at roughly `1..3ms`; the socket reached about `4.18MB` before the timed
close. Supply into the Exit socket, not Exit-to-Target delivery, was limiting.

This rejects operator error, the Mac physical path, Target, Exit-to-Target,
TUN, D16 downlink, Endpoint conservation, routes, process, and cleanup.

## Selected Cause And Architecture

Quinn stores application-limited state as a later connection-wide snapshot.
The service-turn packets were tagged and deliberately filled the snapshotted
window, but an empty transmit poll could set the global state to
application-limited before their delayed ACKs arrived. Those tagged ACKs then
lost part of their Cubic slow-start authority. A deterministic `164ms` RTT
replay reproduced the missing ownership at:

```text
initial_cwnd=12,000B
turn_acked=13,068B
final_cwnd=23,616B
```

The repair uses the existing per-packet tag at ACK time. Only a tagged
service-turn ACK overrides the later global application-limited snapshot;
ordinary packets, Cubic, turn size, deadline, failure rules, Endpoint, D16,
MTU, pool, windows, chunk, GSO, workload, and SLIs are unchanged.

The observer is stopped and bundled. One fresh paired qualification is
required before formal M2; do not repeat this exact source or tune a frozen
value.
