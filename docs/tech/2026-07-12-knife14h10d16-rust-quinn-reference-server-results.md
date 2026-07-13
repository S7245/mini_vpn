# Knife14 H10d16 Rust/Quinn Reference-Server Results

Date: 2026-07-12
Probe source: `0f07406c07a81a373a9b3dc426c4ad0a7128400a`

## Verdict

The independent Rust/Quinn TUIC v5 reference server removed the external
multi-second starvation pattern, but did not meet the Gate A capability floor.
The exact direct-TUIC probe reached `112.559 Mbit/s` receiver over 20 seconds.
All `20/20` client intervals carried data, the minimum interval was `103.805
Mbit/s`, and the data-stream maximum read gap was only `27ms`.

This is a major boundary change from sing-box/Mihomo, which delivered only
`2-5 Mbit/s` with target-side zero-rate intervals and read gaps above three
seconds. It proves that the quic-go server lineage was responsible for the
burst/idle starvation shape on this external path. It does not prove sufficient
capacity: `112.559 Mbit/s` is below the literal `>150 Mbit/s` precondition, so
Gate A and Gate B remain frozen.

The tested reference server is explicitly described upstream as minimal and
not production-ready. It uses Quinn `0.10.1`, whereas mini_vpn and the already
successful minimal-Quinn path use Quinn `0.11.x`. The next discriminator should
use a maintained TUIC v5 server on modern Quinn rather than tune the old
reference binary or reopen D16.

## Code-level reachability review

The official `tuic-server 1.0.0` source at
`a299ef02d9f4ab5a0edbf58b9998f41bf768b332` showed:

- Quinn `0.10.1`, Tokio `1.28.2`, and TLS 1.3;
- direct per-Connect `tokio::io::copy_bidirectional` with no application
  pacing, timer, or intermediate message queue;
- Cubic support;
- default `16 MiB` send window and `8 MiB` per-stream receive window;
- 32 initial bidirectional streams, dynamically increased under pressure;
- configurable ALPN, auth timeout, negotiation timeout, and idle timeout.

At `170 Mbit/s`, the target application rate is `21.25 MB/s`. A `16 MiB` send
window represents about `0.79s` of target throughput, while measured RTT is
approximately `1-2ms`. The configured windows and two-stream requirement were
therefore statically sufficient. The exact source and config semantics are in
the official [reference-server README](https://github.com/EAimTY/tuic/blob/a299ef02d9f4ab5a0edbf58b9998f41bf768b332/tuic-server/README.md)
and [server implementation](https://github.com/EAimTY/tuic/blob/a299ef02d9f4ab5a0edbf58b9998f41bf768b332/tuic-server/src/server.rs).

## Fixed test shape

- exact client binary SHA-256
  `11537685e7736e0ab22bdd602c9e37990eb14c7045e94cd91e0a6af63c724da8`;
- client source `0f07406`, Cubic, safe MTU 1200, `tcp_pool=1`, generic
  OrderedJoin;
- `.27` client, `.111:8443` Exit, `.77:5201` target;
- official `tuic-server 1.0.0` GNU x86_64 binary and checksum;
- server binary SHA-256
  `7cd85d8857cef7990ce067d8b48595e6532f0440522529d796d3a8b2f29e7b9f`;
- server Quinn `0.10.1`, Cubic, `16 MiB` send window, `8 MiB` receive window,
  30-second idle timeout, and exact existing ALPN/auth/TLS identity;
- one 20-second reverse P1 requiring receiver `>150 Mbit/s` and every interval
  nonzero;
- temporary `.111` 16 MiB socket maxima and 1 MiB defaults;
- config, authentication, certificate, and private key held in memory/FIFO
  channels only;
- transient service and bounded restore watchdog.

Immediately preceding direct receiver baselines were `217.640 Mbit/s` from
`.27` and `216.382 Mbit/s` from `.111`.

## Compatibility preflight

The official reference `tuic-client 1.0.0`, SHA-256
`8224772f1f363ee94870ca7b341845096dc1a0c99916fe9319e330c2d32ed0e7`,
ran with an in-memory configuration on `.27`. Its local SOCKS listener opened a
TUIC v5 Connect through `.111` to `.77:22` and read the `SSH-` banner. This
proved TLS, ALPN, auth, Connect, and target forwarding without running a timed
capacity harness.

## Official result

Client iperf3 JSON:

```text
sender_mbps=119.037178
receiver_mbps=112.559158
sent_bytes=298188800
received_bytes=281411584
retransmits=83
intervals=20
zero_intervals=0
min_interval_mbps=103.804978
max_interval_mbps=118.491101
```

Client interval rates in Mbit/s:

```text
113.136,103.805,114.300,114.294,114.382,108.963,112.204,113.247,
108.002,116.387,110.099,112.198,115.343,113.250,118.491,106.953,
114.293,113.249,117.440,111.150
```

The data Connect stream read `281605317B` and had a maximum read gap of `27ms`.
Client Quinn reported zero lost bytes, zero congestion events, and zero
connection/stream data-blocked frames. Those loss counters describe the client
send side; they do not prove the server's downlink loss state.

The final client UDP socket snapshot had received `363397062B`, about `1.29x`
the data-stream bytes. This is consistent with substantial downlink transport
overhead, retransmission, or duplicate reception, but the old reference server
does not expose sufficient transport counters to classify it exactly.

The `.77` sender was continuous and alternated mostly between about `101` and
`118 Mbit/s`, with `0/20` zero intervals. Its aggregate sender was `119
Mbit/s`, retransmissions were `83`, and TCP cwnd stayed about `1.09 MiB`. The
target TCP flow was continuously backpressured at the reference server's
TUIC/QUIC service capacity rather than starved for multiple seconds.

`.111` reported zero `UdpInErrors`, `UdpRcvbufErrors`, and `UdpSndbufErrors`.
The transient server accumulated about `2.73s` of CPU time over its complete
lifetime, including preflight and the official run, with peak memory below
`5 MiB`; there is no evidence of CPU saturation.

The timed iperf shutdown reset one relay and the server logged peer-driven
closure. This terminal classification is secondary: the strict capacity gate
had already failed at `112.559 Mbit/s`.

## Capture correction

The planned bilateral captures were empty. Their 55-second timeout started
before the two direct baselines and orchestration delays, and expired before
the official flow began. They are retained only as failed-instrumentation
evidence and are not cited as packet evidence.

A later one-byte, non-capacity probe with `tcpdump -i any` captured `.27` to
`.111:8443` on `eth0`, zero kernel drops, and showed that the destination is
translated to `.111`'s private interface address before capture. Future runs
must:

- use `-i any` on both endpoints;
- filter primarily by UDP port, not both public host addresses;
- allow at least 120 seconds;
- prove capture readiness and record remaining time immediately before the
  measured command;
- start baselines before, not inside, the measured capture window.

## Architecture and route review

The experiment changes the diagnosis from one undifferentiated external TUIC
failure into two distinct server-transport classes:

| Server class | Receiver | Shape | Active limitation |
|---|---:|---|---|
| sing-box/Mihomo, quic-go `0.59.x` forks | `2-5 Mbit/s` | multi-second burst/idle | external server starvation |
| reference Rust TUIC, Quinn `0.10.1` | `112.559 Mbit/s` | continuous, `27ms` max gap | insufficient server transport capacity/efficiency |
| minimal Quinn `0.11.x` without TUIC | `192.597 Mbit/s` | continuous | path capacity proven |
| host-local sing-box TUIC | `199.639 Mbit/s` | continuous | protocol/copy capacity proven locally |

No result points back to D16 ownership, TUN, smoltcp, native readers, auxiliary
pool policy, MTU, broad client windows, chunk size, or self-wake. Do not modify
those paths.

## Gate decision and next plan

Gate A is not authorized because the receiver did not exceed `150 Mbit/s`.
The zero-interval condition did pass.

The next candidate is maintained Rust server Shoes `v0.2.7`. Its official
[repository](https://github.com/cfal/shoes) documents TUIC v5, prebuilt Linux
binaries, config dry-run, and a direct server mode. Its pinned `v0.2.7`
`Cargo.lock` uses Quinn `0.11.9` and quinn-proto `0.11.13`, close to mini_vpn's
already-qualified Quinn generation. The official x86_64 GNU asset SHA-256 is
`134974a4640807bb6767bf4b35f7a71637cdbbb96b8bef622ef86cf197220aac`.

Before one Shoes run:

1. review its TUIC Connect copy path, socket setup, transport windows, and
   Cubic/MTU behavior;
2. dry-run an in-memory secret-free configuration and repeat the bounded SSH
   banner compatibility preflight;
3. run direct baselines before a 120-second, readiness-proven bilateral
   capture window;
4. run the exact `0f07406` 20-second strict discriminator once.

If Shoes exceeds `150 Mbit/s`, every interval is nonzero, and drop/error gates
are clean, keep that exact service alive and run one composite Gate A. If it
fails, stop server substitution and use the complete capture/transport evidence
to choose between a modern Quinn TUIC implementation defect and the external
path interaction.

## Artifacts

- client bundle:
  `/tmp/mini-vpn-rust-tuic-client.tar.gz`
  (`f0124a4db718c335ccf4a41d5e290dc609fa42f5dbba43a5d18592933662231f`);
- redacted server bundle:
  `/tmp/mini-vpn-rust-tuic-server.tar.gz`
  (`e0d5276a18776393b2675a23d65f42d02f0b76f410496e59f3d12376563bcde2`);
- target journal:
  `/tmp/mini-vpn-rust-tuic-target.log`
  (`650cc3f24321ce15691034993ca72e9d29316a3cb1d226792cc753fe335f69a5`).

The server journal and status were UUID-redacted before packaging and then
checked for UUID-shaped strings. The bundles exclude configuration payloads,
environment files, credentials, certificates, private keys, and sudo input.

## Cleanup

- reference server/client binaries, launchers, runtime FIFOs, transient units,
  watchdogs, pcaps, and remote artifacts were removed;
- `.111:8443` was released and all four socket sysctls restored to `212992`;
- `.27` exact-source clone, bundle, preflight files, logs, and remote artifacts
  were removed after verified local collection;
- `.77` iperf3 remained active and unchanged;
- no macOS TUN test ran and no product source changed.
