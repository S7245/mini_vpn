# Knife14 H10d16 Client-Host Discriminator Results

Date: 2026-07-12
Source: `f1db4e477deb97b06628901cd4dbd7522a4bc030`

## Verdict

Changing the mature-client host from `.27` to `.33` did not restore the
Gate-aligned TUIC control. With the same `.111` Exit, `.77` target, sing-box
`1.13.14`, `Cubic + MTU1200` client profile, and strict authorization floor,
the `.33` control reached only `5.347 Mbit/s` receiver.

The direct receiver was `218.285 Mbit/s`, both UDP sockets had `16 MiB`
buffers with drop `0`, the target-only and Exit routes were correct, and the
client logged no error. Thirteen of twenty receiver intervals were exactly
zero. The `.111` server logged only the expected timed stream cancellation.

This rejects `.27` host state or the `.27 <-> .111` path as a sufficient root.
It also rejects moving Gate A to `.33` as a solution. The clean `bdaa19c`
scoped run, composite Gate A, and Gate B remain unspent.

## Fixed discriminator

Only the mature-client host changed:

- previous client: `.27` (`43.172.75.27`);
- discriminator client: `.33` (`43.153.32.33`);
- Exit: `.111` (`43.173.101.111`), unchanged minimal FIFO-only TUIC service;
- target: `.77` (`43.130.32.77`), unchanged iperf3 service;
- mature binary: sing-box `1.13.14`, unchanged SHA-256
  `4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`;
- client profile: `gate-aligned-mtu1200-cubic-reverse-p1`;
- traffic: one `20s` reverse P1;
- authorization: receiver strictly greater than `150 Mbit/s`, with both UDP
  socket drops zero.

The versioned control ran from a clean git-bundle clone at `f1db4e4`. Its
self-test and Bash syntax check passed before VPS traffic.

## Safety and preflight

- `.33` existing sing-box service was active before and after the control and
  was never stopped;
- `.33 -> .111:8443` ingress-only capture observed the UDP arrival with kernel
  drop `0` before the temporary service started;
- `.111` started with zero error lines and a 60-minute cleanup watchdog;
- server configuration, certificate, and private key were consumed through
  separate mode-0600 FIFOs and then removed;
- TUIC client credentials and SNI moved from `.27` to `.33` through one
  mode-0600 FIFO and were not persisted;
- the SSH private key remained in a temporary local ssh-agent; `.33` received
  only the corresponding public key file for nested socket evidence;
- `.77` iperf3 was active;
- `.33` source tree was clean and the runner/binary hashes were recorded.

## Result

Observed control evidence:

- direct sender/receiver: `223.895/218.285 Mbit/s`;
- TUIC sender/receiver: `7.025/5.347 Mbit/s`;
- target route: `.77` through `sb-d16-control`;
- Exit route: `.111` through `eth0`;
- actual TUN MTU: `1200`;
- client UDP socket: `rb=16777216 tb=16777216 drop=0`;
- Exit UDP socket: `rb=16777216 tb=16777216 drop=0`;
- zero-rate receiver intervals: `13/20`;
- maximum one-second receiver interval: `27.263 Mbit/s`;
- nonzero interval median: `10.487 Mbit/s`;
- client error lines: `0`;
- Exit error: expected timed stream cancellation only.

Comparison across `.111` controls:

| Client | Profile | Receiver | Zero intervals | Maximum interval |
|---|---|---:|---:|---:|
| `.27` | historical BBR/1500 | `4.928 Mbit/s` | `13/20` | `39.853 Mbit/s` |
| `.27` | Gate-aligned Cubic/1200 | `3.041 Mbit/s` | `14/20` | `37.749 Mbit/s` |
| `.33` | Gate-aligned Cubic/1200 | `5.347 Mbit/s` | `13/20` | `27.263 Mbit/s` |

The repeated burst/idle signature survives both profile and client-host
changes. Direct TCP capacity, local routes, socket sizing, kernel socket drops,
the historical BBR profile, `.27` host state, and mini_vpn are not sufficient
explanations.

Local artifact:

`/tmp/mini_vpn_h10d16_exit111_client33_gate_aligned_f1db4e4.tar.gz`

SHA-256:

`26cdacce4c169217f88512c91127b624728a0c4a9034c53576e69251e90cb104`

## Code review

The versioned control reached the intended public behavior and contains no
route/profile/authorization defect that explains a shared multi-second stall.
The mature client, not mini_vpn, owns the TUN and TUIC path in this test. No D16
reader, byte ledger, leased queue, actor, smoltcp, or mini_vpn TUN code ran.

Therefore this result cannot justify changing D16 ownership, queue capacity,
actor cadence, EOF handling, Quinn windows, MTU, chunk size, self-wake, pool
selection, or stale-slot policy.

## Proposed next discriminator

Before another TUIC or mini_vpn run, measure the raw reverse UDP path on the
same allowed `.111:8443` port:

1. temporarily run one watchdog-protected iperf3 server on `.111:8443`, with
   no TUIC process active;
2. prove TCP and UDP arrival at `.111:8443` before measurement;
3. from `.27` and `.33`, run fixed `20s`, `1200B` reverse UDP tests at bounded
   offered rates, starting at `100 Mbit/s` and then `200 Mbit/s` only if the
   first rate has low loss;
4. record sender/receiver rate, packet loss, jitter, interval shape, socket
   drops, source/destination route, and exact iperf3 version;
5. if raw UDP also stalls or loses heavily on both clients, classify the
   provider/path as the external blocker and replace that path before Gate A;
6. if raw UDP is clean near the offered rate, the next seam is a minimal
   same-version QUIC benchmark without TUIC/TUN, with bilateral capture to
   separate QUIC loss/retransmission from TUIC stream service.

This is an external path discriminator, not permission to weaken the mature
floor. Gate A remains frozen until an immediately preceding Gate-aligned mature
control exceeds `150 Mbit/s` with both socket drops zero.

## Cleanup

- `.111` temporary TUIC process and watchdog stopped;
- `.111` UDP `8443` listener removed;
- `.111` socket maxima/defaults restored to `212992`;
- `.111` binary, log, runtime directory, and FIFO paths removed;
- `.33` control TUN, temporary clone, bundle, public key, credential FIFO, and
  remote artifact removed;
- `.33` existing sing-box remained active and its persistent socket settings
  remained `16 MiB` maxima and `1 MiB` defaults;
- no credential, private key, environment value, or sudo password was written
  into the repository or result artifact.
