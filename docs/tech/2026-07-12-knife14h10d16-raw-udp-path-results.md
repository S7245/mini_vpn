# Knife14 H10d16 Raw UDP8443 Path Results

Date: 2026-07-12
Source: `df3a471313e48882ed7eda76ed35774e15ebe284`

## Verdict

The raw reverse UDP path from `.111:8443` has continuous capacity above the
Gate A and Gate B throughput targets. Both `.27` and `.33` received a fixed
100 Mbit/s offered load at approximately `105 Mbit/s` for every one-second
interval with zero packet loss. At a 200 Mbit/s offered load, both clients
received approximately `198 Mbit/s` overall with no zero-rate interval, but a
shared shaping edge appeared after five seconds.

The two 200 Mbit/s runs were nearly packet-identical: the first five seconds
received about `210 Mbit/s` with zero loss, then the path settled near `193
Mbit/s` with `7.9%` per-interval loss. Overall loss was `5.6%`. Client kernel
`UdpInErrors`, `UdpRcvbufErrors`, and `UdpSndbufErrors` deltas were all zero.

This rejects raw UDP capacity or client receive-buffer drops as the cause of
the mature TUIC `3-5 Mbit/s` burst/idle controls. The path has enough continuous
capacity for `>150 Mbit/s` Gate A and the `170 Mbit/s` Gate B target, while the
shared `.111` egress/provider edge around `193 Mbit/s` remains relevant
headroom evidence. The next discriminator is QUIC without TUIC or TUN.

## Why iperf2 was required

The first preflight used iperf3 on `.111:8443`. Its UDP data mode still
requires a TCP control connection. Both `.27` and `.33` TCP connections timed
out even while the server was listening, and an ingress-only `.111` capture
observed no TCP SYN. UDP probes from both clients had already reached the same
port. The cloud/upstream policy therefore exposes UDP8443 but not TCP8443.

Iperf2 `2.1.9` supports UDP reverse mode over the single UDP socket and does
not need the blocked TCP control path. The same Ubuntu package version was
temporarily installed on `.27`, `.33`, and `.111`, then purged after evidence
collection. The official behavior is documented at:

`https://iperf2.sourceforge.io/iperf-manpage.html`

## Fixed test shape

- server: `.111`, UDP `8443` only;
- clients: `.27`, then `.33`, strictly serial;
- direction: reverse, `.111 -> client`;
- iperf2: `2.1.9` on every host;
- datagram length: `1200B`;
- duration: `20s` per capacity run;
- socket buffer request: `8 MiB`, observed Linux socket buffer `16 MiB`;
- first offered rate: `100 Mbit/s`;
- second offered rate: `200 Mbit/s`, only after the first rate passed;
- one-second enhanced interval reporting;
- client UDP kernel counters captured before and after each run.

A one-second 1 Mbit/s tracer bullet first proved that `--reverse` sent from
`.111` to `.27`, used the expected socket buffer, and reported receiver loss
and jitter.

## Results

| Client | Offered | Receiver | Loss | Jitter | Interval shape |
|---|---:|---:|---:|---:|---|
| `.27` | `100 Mbit/s` | `105 Mbit/s` | `0/218456 (0%)` | `0.064 ms` | 20/20 stable |
| `.33` | `100 Mbit/s` | `105 Mbit/s` | `0/218456 (0%)` | `0.070 ms` | 20/20 stable |
| `.27` | `200 Mbit/s` | `198 Mbit/s` | `24274/436911 (5.6%)` | `0.035 ms` | continuous; shared edge |
| `.33` | `200 Mbit/s` | `198 Mbit/s` | `24274/436910 (5.6%)` | `0.035 ms` | continuous; shared edge |

For both 200 Mbit/s runs:

- seconds 0-5: approximately `210 Mbit/s`, loss `0%`;
- second 5-6: approximately `207 Mbit/s`, loss `1.1%`;
- seconds 6-20: approximately `193 Mbit/s`, loss `7.9%`;
- zero-rate intervals: `0`;
- multi-second stalls: `0`;
- client `UdpInErrors` delta: `0`;
- client `UdpRcvbufErrors` delta: `0`;
- client `UdpSndbufErrors` delta: `0`.

The identical transition and loss count on two independent clients point to a
shared `.111` sender/egress/provider policy rather than either client kernel.
It is a high-rate shaping edge, not the TUIC long-gap failure: the receiver
continues near `193 Mbit/s` instead of falling to zero for many seconds.

## Artifacts

- `.27` client:
  `/tmp/mini_vpn_raw_udp8443_client27.tar.gz`
  (`f4eace0a728a43a8f4710fea6d9e2c500efa0ea766e9f927999d2cf454f89287`)
- `.33` client:
  `/tmp/mini_vpn_raw_udp8443_client33.tar.gz`
  (`a770d20082d7885044dac3cd85066f1c62e6e9bfdfbf0050fa61f60d876b6f00`)
- `.111` server:
  `/tmp/mini_vpn_raw_udp8443_exit111.tar.gz`
  (`95400c503a0a6a58a5f0e013b40c225983ce1afc31af85f420b9d9118d2d9548`)

## Review

This result does not execute mini_vpn, TUIC, Quinn, TUN, smoltcp, D16, or the
egress actor. It is a system-boundary discriminator. It proves sufficient raw
packet capacity but cannot distinguish QUIC recovery from TUIC stream service.

Do not change D16 ownership, queue capacity, actor cadence, EOF, MTU, broad
windows, chunk size, self-wake, pool selection, or stale-slot policy from this
result. The new evidence moves the next seam one layer upward from UDP to QUIC.

## Proposed minimal Quinn stage

Add a versioned test-only cross-host Quinn reverse-stream probe, with no TUIC,
TUN, smoltcp, or D16 logic:

1. keep probe logic under `#[cfg(test)]` so it cannot enter mobile/product
   binaries or expand the core runtime API;
2. reuse the existing private QUIC transport builder with `Cubic`,
   `Safe1200`, current stream/connection receive windows, send window, and
   socket-buffer policy;
3. RED/GREEN a real loopback reverse-stream test that verifies request framing,
   continuous ordered delivery, EOF, byte equality, interval accounting, and
   final Quinn statistics;
4. add one ignored role-driven cross-host test that runs the same compiled test
   binary as server on `.111` and client on `.27`/`.33`;
5. stream certificate/key through mode-0600 FIFOs and preserve exact
   source/binary hashes;
6. run one `20s` `.27 -> QUIC .111` reverse test first; require receiver
   `>150 Mbit/s`, no multi-second zero interval, and explainable Quinn
   loss/congestion/blocking counters;
7. run `.33` only if `.27` needs a client-host discriminator;
8. if minimal Quinn is healthy, the remaining failure is TUIC/server stream
   service and should be compared against sing-box before touching D16;
9. if minimal Quinn reproduces the stalls, diagnose Quinn/path behavior using
   bilateral captures and connection statistics before product changes.

Gate A remains frozen until a Gate-aligned mature control passes immediately
before clean `bdaa19c` scoped acceptance.

## Cleanup

- `.111` iperf2 server and watchdog stopped;
- `.111` UDP8443 listener removed;
- `.111` socket maxima/defaults restored to `212992`;
- `.27`, `.33`, and `.111` iperf2 packages purged;
- iperf3 remained installed and unchanged;
- `.33` existing sing-box remained active;
- remote artifact directories and archives removed after verified local copy;
- no credential, private key, environment value, or sudo password entered an
  artifact or repository file.
