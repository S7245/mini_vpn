# Knife15 M2 Endpoint-Blocked App-Limited Qualification Failure Results

Date: 2026-08-08

Status: **FAILURE CLASSIFIED; ENDPOINT-BLOCKED APP-LIMITED OWNERSHIP SELECTED; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260808_065723.tar.gz`.

Mac SHA-256:
`6fae3611b0a58d6b668c0ce85ee3f4dc5b08f0191dd21d48b5c954263a31630e`.

Exact source: `a7cd603f40aa4a59916e37f82a9eed8f0720ce68`.

Paired Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260808_063213.tar.gz`.

Exit SHA-256:
`ffb39a4826b17ed976606af6307672dfb2deff51c49a370eed9d321c2875c670`.

## Accepted Controls

- direct baseline: `28.490/52.003 Mbit/s`;
- 300-second direct forward: `13.970 Mbit/s`, without a complete receiver gap;
- start, smoke, IPv6, full-tunnel, real-client, route, TUN, process, and
  interface preflights: PASS;
- cycle-1 300-second forward and reverse TCP: complete without receiver-zero
  intervals;
- cycle-1 reverse UDP: complete at `1.615830%` loss;
- cleanup: route, DNS, TUN, process, and Endpoint ownership PASS.

Endpoint maximum/final ownership was `61,440B / 61,403/0/0B`. Formal M2 was
not run.

## Exact Failure

The first ten-second short forward failed its unchanged Target continuity SLI:

```text
Mac sender/accepted:       10,092,544B
Target received:            4,063,232B
Mac sender-zero intervals:           3
Target receiver-zero intervals:      1
D16 writer progress:        5,773,827B
D16 writer max wait:        3,035,698us
```

The exact owner was conn1 generation 2. Its corrected successor service turn
succeeded at `target/sent/acked/lost = 12,000/12,800/12,800/0B` and installed
with the required `24,800B` cwnd. This satisfies the previous implementation
invariant and triggers its stop rule: the fixed one-turn architecture is
rejected without increasing its target, adding turns, retrying, or tuning.

The business data stream then remained continuously writer-Pending. Its exact
transport acknowledgements advanced in every observation, but only reached
about `3.93MB` before close. The connection grew from `24,800B` to `274,679B`
only after the ten-second flow and one small congestion event. Endpoint bulk
grants advanced from about `78,808B` to `4,576,336B`; every transmit path used
the Endpoint waiter.

## Paired Exit Evidence

The observer captured `2,075,057` packets with zero capture/kernel drops. The
failed data socket was `172.26.0.2:43392 -> 43.130.32.77:5201`:

```text
SYN:                         07:11:33.104671Z
first bulk byte:             07:11:33.275959Z
first 128KiB boundary:       07:11:34.313480Z
Target ACK RTT:              about 1ms
```

Exit supplied data continuously once bytes arrived, and Target ACKed the
observed supply. The first 128KiB nevertheless required about `1.21s` after
the data socket SYN, causing iperf's first complete 128KiB read block to fall
outside the first one-second receiver interval.

This rejects operator, physical-link, Target, Exit-to-Target, TUN, D16
downlink, Endpoint conservation, routes, process, and cleanup branches. The
limiting seam remains client-to-Exit business-stream congestion growth.

## Selected Cause

`Connection::poll_transmit` records
`app_limited = buf.is_empty() && !congestion_blocked`. When STREAM bytes are
sendable but the endpoint pre-accounting service temporarily denies the next
datagram reservation, `endpoint_blocked=true`, `congestion_blocked=false`, and
the empty poll incorrectly publishes `app_limited=true`.

The later ACK is therefore allowed to skip Cubic slow-start growth even though
the application writer was continuously Pending. Endpoint token ownership is
transport backpressure, not absence of application data. The previous tagged
service-turn repair was exact for that synthetic flight but could not correct
ordinary business packets.

The selected repair is the minimal ownership rule; control-only Endpoint
waits retain upstream behavior:

```text
Endpoint-bulk-blocked transmit poll != application-limited transmit poll
```

No rate, burst, service-turn, pool, RTT, timer, or workload value is selected.
