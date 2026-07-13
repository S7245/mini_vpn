# Knife14 H10d16 Mihomo Alternate-Server Results

Date: 2026-07-12
Probe source: `0f07406c07a81a373a9b3dc426c4ad0a7128400a`

## Verdict

The first alternate mature TUIC server A/B did not authorize Gate A. Replacing
sing-box `1.13.14` with official Mihomo `v1.19.28` on the same `.111:8443`
external path produced the same burst/idle failure. The exact direct-TUIC
probe reached only `3.460 Mbit/s` receiver over 20 seconds, with `7/20` client
intervals at zero and a maximum data-stream read gap of `3431ms`.

The target-side result was even sharper: `.77` sent only `5.18 Mbit/s` and had
`14/20` zero-rate intervals. Direct reverse baselines immediately before the
probe were `217.010 Mbit/s` from `.27` and `216.591 Mbit/s` from `.111`.
Client Quinn loss, congestion, and data-blocked counters were zero; `.111` UDP
kernel error counters were also zero.

This rejects a sing-box-application-specific server/copy root. It does not yet
reject the quic-go transport lineage: Mihomo `v1.19.28` depends on a
`metacubex/quic-go 0.59.1` development fork, while sing-box `v1.13.14` depends
on a `sagernet/quic-go 0.59.0` fork. The next discriminator must either change
the server QUIC lineage or add bilateral transport timing/telemetry. D16 and
Gate A/B remain frozen.

## Fixed A/B shape

- exact client release test binary SHA-256
  `11537685e7736e0ab22bdd602c9e37990eb14c7045e94cd91e0a6af63c724da8`;
- client source `0f07406`, Cubic, safe MTU 1200, `tcp_pool=1`, generic
  OrderedJoin;
- `.27` client, `.111:8443` Exit, `.77:5201` target;
- one 20-second reverse P1 with receiver `>150 Mbit/s` and every interval
  nonzero required;
- official Mihomo `v1.19.28` `linux-amd64-compatible` asset, compressed
  SHA-256
  `70d01cfb8cb7bf7a92fd1af16cb4b9553d90bb4eecde3b5c4849103e27c80ddb`;
- decompressed Mihomo binary SHA-256
  `8abefa5078cebfa434995dccec8d7f20925450c27df2bc88aa28f0db9f8ed7d9`;
- Mihomo TUIC v5 listener with Cubic and the existing ALPN/auth/TLS identity;
- direct outbound mode to the target;
- 16 MiB temporary `.111` socket maxima and 1 MiB defaults;
- secret-bearing configuration held in an anonymous memory file and TLS
  material delivered through root-only, one-shot FIFOs under Mihomo's home;
- bounded transient service and restore watchdog.

The release and listener schema were taken from the official
[Mihomo releases](https://github.com/MetaCubeX/mihomo/releases/tag/v1.19.28)
and [Mihomo inbound documentation](https://wiki.metacubex.one/en/config/inbound/).

## Preflight

The first service start did not listen because Mihomo's `SAFE_PATHS` rejected
certificate paths under `/proc/self/fd`. No client traffic was sent. The
launcher was corrected to expose the certificate and private key through
one-shot FIFOs below the configured Mihomo home; no key bytes were persisted.

A one-second compatibility preflight then established TLS/TUIC v5 auth, opened
both iperf control/data Connect streams, and reached `.77`. The short timed
close caused one relay `Broken pipe`, so the strict capacity harness correctly
failed it. This preflight is only compatibility evidence and is not counted as
a throughput result.

## Official result

Client iperf3 JSON:

```text
sender_mbps=5.180148
receiver_mbps=3.460124
sent_bytes=12976128
received_bytes=8650752
retransmits=16
intervals=20
zero_intervals=7
min_interval_mbps=0.000000
max_interval_mbps=18.874670
```

Client interval rates in Mbit/s:

```text
7.333,1.049,0,0,18.875,5.243,0,0,11.533,0,
0,7.340,0,6.291,2.097,2.097,2.097,2.097,2.097,1.049
```

The data Connect stream read `8785854B`; its maximum read gap was `3431ms`.
The control and data streams opened on one authenticated connection. The timed
iperf shutdown ended one relay with `Broken pipe`, but the capacity verdict had
already failed because seven measured intervals were zero and aggregate
receiver throughput was far below the floor.

Client Quinn reported throughout the run:

```text
lost_bytes=0
congestion_events=0
tx_blocked(data=0,stream=0)
```

The `.77` sender independently reported:

```text
sender_mbps=5.18
retransmits=16
zero_intervals=14/20
tcp_cwnd_while_idle=about_589_KiB
```

Its nonzero bursts appeared near seconds `0`, `5`, `8`, `11`, `13`, and `18`.
Mihomo logged both TCP Connect streams as `DIRECT`; it emitted no active
service error. `.111` reported zero `UdpInErrors`, `UdpRcvbufErrors`, and
`UdpSndbufErrors`.

## Review

The result removes these roots from the active set:

- D16 ownership, actor cadence, DrainOnly, EOF, TUN, and smoltcp;
- mini_vpn native readers and auxiliary pool policy;
- sing-box-specific TUIC ingress or target-copy implementation;
- client/server socket drops, direct target capacity, and generic raw path
  capacity;
- server BBR, because both alternate runs used Cubic.

The A/B changed the application/server implementation but not the transport
family. Official dependency manifests show:

- [Mihomo `v1.19.28` go.mod](https://raw.githubusercontent.com/MetaCubeX/mihomo/v1.19.28/go.mod):
  `github.com/metacubex/quic-go` based on `0.59.1`;
- [sing-box `v1.13.14` go.mod](https://raw.githubusercontent.com/SagerNet/sing-box/v1.13.14/go.mod):
  `github.com/sagernet/quic-go` based on `0.59.0`.

Therefore the strongest remaining hypothesis is an external-path interaction
shared by the quic-go server lineage or by TUIC server scheduling above it.
The evidence does not justify changing Quinn receive windows, D16 queues, MTU,
chunk size, self-wake, VPS buffers, or iperf parameters.

## Gate decision and next plan

Gate A is not authorized. The alternate server failed both required capacity
conditions: receiver was not above `150 Mbit/s`, and not every interval carried
data.

Before the next measured run:

1. review a TUIC v5 server with an independent QUIC stack; the official
   reference `tuic-server 1.0.0` is Rust/Quinn and is a candidate, although its
   age and fixed config/window semantics must be reviewed first;
2. TDD its exact auth/TLS/Connect compatibility without reusing a one-second
   capacity harness as an auth probe;
3. add synchronized `.27`/`.111` packet captures, target sender intervals, and
   server transport telemetry to the single cross-host run;
4. preserve exact `0f07406`, Cubic/safe1200, `.111:8443`, `.77:5201`, socket
   buffers, and strict floor.

The reference server is documented by the official
[TUIC v5 release](https://github.com/EAimTY/tuic/releases/tag/tuic-server-1.0.0)
as a minimal Rust/Quinn implementation. If an independently transported server
passes `>150 Mbit/s`, zero interval, and zero-drop gates, that same capable
server may qualify one composite Gate A. If it fails, compare the bilateral
timing with minimal Quinn before considering any product-code change.

## Artifacts

- client bundle:
  `/tmp/mini-vpn-mihomo-alt-client.tar.gz`
  (`97e8b4275343ba197edef3d03acb1e670fbd9a454d8508b40f2faf80e3932044`);
- server bundle:
  `/tmp/mini-vpn-mihomo-alt-server.tar.gz`
  (`b31e0802b63a93705ee1b57cac9e3f781001a69894bfb69e2af6ac82c961614d`);
- target journal:
  `/tmp/mini-vpn-mihomo-alt-target.log`
  (`826b47b00ef05dc5e0a26ce11c7a8a067d37f0958c9df5626eb60d7c56a90f76`).

The bundles exclude configuration payloads, environment files, credentials,
certificates, private keys, and sudo input.

## Cleanup

- Mihomo, launcher, transient unit, watchdog, runtime FIFOs, and remote
  artifacts were removed from `.111`;
- `.111:8443` was released and all four socket sysctls restored to `212992`;
- the `.27` exact-source clone, bundle, logs, and remote artifacts were
  removed after verified local collection;
- `.77` iperf3 remained active and unchanged;
- no macOS TUN test ran and no product source changed.
