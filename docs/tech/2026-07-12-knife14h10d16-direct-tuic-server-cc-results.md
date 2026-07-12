# Knife14 H10d16 Direct TUIC And Server-CC Results

Date: 2026-07-12
Probe source: `0f07406c07a81a373a9b3dc426c4ad0a7128400a`

## Verdict

The test-only direct TUIC Connect probe reproduced the low-capacity failure
without TUN, smoltcp, native readers, D16, the product event loop, or an
auxiliary TCP-pool slot. With `tcp_pool=1`, generic OrderedJoin, client Cubic,
safe MTU 1200, and the same temporary `.111` sing-box Exit, it reached only
`3.775 Mbit/s` receiver when the server used BBR.

Changing the sole server variable from BBR to Cubic did not restore capacity.
The exact same test binary then reached only `2.674 Mbit/s`. Server Cubic
changed the delivery shape: client-side zero-rate intervals fell from `14/20`
to `0/20` and maximum data-stream read gap fell from `3441ms` to `225ms`.
However, the `.77` iperf3 sender still sent in bursts separated by several
zero-rate seconds, so sing-box buffered and smoothed the data without draining
the target TCP socket at useful capacity.

Server BBR is therefore one cause of the multi-second burst/idle shape, but it
is not the `>150 Mbit/s` capacity root. The remaining boundary is sing-box TUIC
server stream/copy/flow-control behavior or its interaction with the external
`.111 -> .27` QUIC path. D16 and Gate A remain frozen.

## Fixed probe shape

Both runs used:

- source commit `0f07406` from a clean `.27` clone;
- release test binary
  `target/release/deps/mini_vpn-9e6da801b20652a1`;
- exact binary SHA-256
  `11537685e7736e0ab22bdd602c9e37990eb14c7045e94cd91e0a6af63c724da8`;
- mini_vpn client Cubic and safe MTU 1200;
- `tcp_pool=1` and generic OrderedJoin `open_tcp`;
- one loopback iperf3 process whose control and P1 data sockets each mapped to
  one TUIC Connect stream over the same QUIC connection;
- `.111` sing-box `1.13.14`, SHA-256
  `4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`;
- `.77:5201` iperf3 target;
- 20-second reverse P1 and strict receiver `>150 Mbit/s` gate;
- 8 MiB requested client UDP buffers, observed as 16 MiB;
- 16 MiB temporary `.111` UDP socket maxima and 1 MiB defaults;
- FIFO-only config, certificate, and private-key material;
- bounded service runtime and cleanup watchdog.

The BBR run direct baseline was `219.526 Mbit/s` receiver. The Cubic A/B
direct baseline was `213.236 Mbit/s`. The Cubic run rebuilt in the same source
path as the BBR run and reproduced the exact binary SHA before traffic.

## Direct TUIC result with server BBR

Client iperf3:

```text
sender_mbps=5.494
receiver_mbps=3.775
intervals=20
zero_intervals=14
min_interval_mbps=0.000
max_interval_mbps=36.700
```

Client interval rates were:

```text
4.193,0,0,2.097,0,0,11.534,0,0,0,36.700,0,0,0,18.874,0,0,2.098,0,0
```

The data stream read about `9.56 MiB`; maximum read gap was `3441ms`. Quinn
reported zero lost bytes, zero congestion events, and no sustained
connection/stream data blocking. The local poll gap stayed at approximately
`4ms` or below while self-wake remained active.

The `.77` sender independently showed the same burst/idle pattern: `15/20`
one-second intervals transferred zero bytes, aggregate sender rate was `5.49
Mbit/s`, TCP cwnd remained about `505 KiB`, and retransmissions totaled `6`.
This proves the target TCP sender was backpressured before data reached the
mini_vpn reader.

## Direct TUIC result with server Cubic

Client iperf3:

```text
sender_mbps=5.075
receiver_mbps=2.674
intervals=20
zero_intervals=0
min_interval_mbps=2.096
max_interval_mbps=7.337
```

Client interval rates were:

```text
7.337,3.146,2.096,3.146,2.097,2.097,2.097,2.097,2.097,2.097,
3.146,2.097,3.146,2.097,2.097,3.146,2.097,2.098,3.144,2.097
```

The data stream read about `6.78 MiB`; maximum read gap fell to `225ms` and
client intervals became continuous. Quinn again reported zero lost bytes,
zero congestion events, and no sustained data blocking.

The `.77` sender did not become continuous. It sent `6.25 MiB` in the first
second, `1.38 MiB` in the second, and then about `1.5 MiB` bursts near seconds
`6`, `11`, and `16`; `15/20` sender intervals were zero. Aggregate sender rate
was `5.08 Mbit/s`, cwnd remained about `911 KiB`, and retransmissions totaled
`13`. The difference between target and client intervals shows sing-box
buffering smoothed the external QUIC delivery while its target-facing copy
remained capacity-starved.

Both timed runs ended with the iperf data Connect stream canceled/reset at the
20-second boundary while the control connection closed cleanly. That terminal
shape is recorded separately and does not explain the low middle-window rate.

## Architecture review

The direct probe exercises real mini_vpn TUIC authentication, Connect framing,
one Quinn connection, sing-box TUIC ingress, sing-box direct outbound, and the
real iperf3 server. It deliberately excludes every D16 concern. The shared low
result across mature sing-box clients and mini_vpn generic OrderedJoin removes
the following from the active capacity root set:

- D16 ownership, byte ledger, readiness queue, actor cadence, DrainOnly, and
  EOF ordering;
- TUN and smoltcp;
- native ordered readers and reservation cancellation;
- pool-2 auxiliary selection, stale probing, and reconnect;
- mini_vpn local reader service, because the target sender itself was stalled;
- client BBR/MTU profile and server BBR as sufficient explanations;
- raw UDP capacity and minimal Quinn, already proven near `198` and `192.6
  Mbit/s` respectively.

Do not reopen product egress architecture, broad QUIC windows, MTU, chunk
size, self-wake, or pool tuning from this result.

## Next discriminator

Run one host-local direct-TUIC probe on `.111`:

1. use the same sing-box binary and Cubic server profile;
2. run the exact `0f07406` test binary on `.111` with the TUIC server address
   on loopback and target `.77:5201`;
3. feed credentials and CA material ephemerally; persist no environment or
   secret-bearing config;
4. preserve the 20-second reverse P1, pool=1, OrderedJoin, client
   Cubic/safe1200, exact hashes, interval gate, target-side journal, and bounded
   cleanup;
5. require receiver `>150 Mbit/s` and no client zero interval.

If host-local remains near `2-5 Mbit/s`, the root is inside sing-box TUIC
server/copy/flow-control behavior on this binary/config. The next action should
be mature-server source/version comparison, not mini_vpn changes. If
host-local exceeds `150 Mbit/s`, the root is the sing-box/quic-go server's
interaction with the external `.111 -> .27` QUIC path; collect server-side
QUIC transport evidence or compare one alternate mature TUIC server before
touching product code.

## Artifacts

Server BBR:

- client:
  `/tmp/mini-vpn-direct-tuic-0f07406-client.tar.gz`
  (`9bf2ca9c520c39973dc3a84f4e184a078a002bf0ede048a5085d98b128042b68`);
- server:
  `/tmp/mini-vpn-direct-tuic-0f07406-server.tar.gz`
  (`52c0402253e7aeb25a76f4b4d091a123bf47f2e7ab63737bd32701d6c7a9de6d`).

Server Cubic:

- client:
  `/tmp/mini-vpn-direct-tuic-0f07406-cubic-client.tar.gz`
  (`a094306ae42f16b2c174f3d6becefcb1e6e0cd8efaf324a91a7f368d4913933a`);
- server:
  `/tmp/mini-vpn-direct-tuic-0f07406-cubic-server.tar.gz`
  (`f62862818360381d3bfaa3424da2669dc5d0d5ff914c917fbe50c3e705e4988d`);
- target journal:
  `/tmp/mini-vpn-direct-tuic-0f07406-cubic-target.log`
  (`5486f9f83f3dc1272005156bbe9330c575de9677feac81bfe2cbc06d6fce5642`).

## Cleanup

- both temporary `.111` services and watchdogs stopped;
- `.111:8443` released;
- `.111` socket sysctls restored to `212992` for all four values;
- FIFO paths, secret-bearing material, transient sing-box binaries, remote
  artifacts, and remote clean clones removed;
- `.77` iperf3 remained active and unchanged;
- no credential, private key, environment value, or sudo password entered an
  artifact or repository file.
