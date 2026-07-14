# Knife14 H10d16 Endpoint Pacing Service VPS Completion Results

Date: 2026-07-14
Status: **PASS — Task 12 step 4 product regression complete**

## Scope And Provenance

This stage validates exact repair source
`5f9da90f734b1754fd8c41bcb70fa4c8b6ae9f74` after the accepted forward P1.
It changes no frozen pacing, D16, MTU, TUN, pool, QUIC-window, chunk, Cubic,
GSO, driver-bound, or self-wake constant.

The deployed source archive SHA-256 was
`77a7a7f5c3d8ecadb96531d759a5504826da4e5650c49a35c2c536c3074349c3`.
The release binary remained
`e22d521453afb1e46f2f085cfeb8a60aab95a36849ebf98897b38f7794f4d232`.
The suite and low-RTT probe remained
`7d43d61c0032c933910e7cf667d609f8210b18e068af69262a5978812bebfaa7`
and
`79eab40ed884efb225e830ba9a03b1399257a13cbf47e586a9c5be1766597fdd`.

The valid reverse sender was Shoes v0.2.7 / Quinn 0.11.9 on the authorized
`.111:8443` Exit. Its official archive and deployed binary SHA-256 values were
`134974a4640807bb6767bf4b35f7a71637cdbbb96b8bef622ef86cf197220aac`
and
`160202f6744b14ba84b40c014f3754f7811642300df58df81f1399063a202147`.

## Reverse P8 Topology Correction

The earlier `.33` reverse samples were not valid H10d16 architecture
discriminators. That Exit's sing-box/quic-go reverse sender is independently
known to remain in the low single-digit Mbit/s class on this VPS topology.
The exact client therefore cannot be rejected from a reverse test whose
external sender is already below the gate.

The `.111` Shoes/Quinn Exit is the capable control: its external reverse path
has already demonstrated roughly `192 Mbit/s`. The formal H10d16 reverse P8
was therefore rerun only on that topology.

## Formal Reverse P8

The target-only, reverse-only, eight-flow, 60-second run passed:

- aggregate sender/receiver: `191/188 Mbit/s`;
- all `60/60` aggregate one-second intervals were nonzero; minimum was
  `149 Mbit/s`;
- all eight flows exceeded one `128 KiB` D16 quantum; receiver totals were
  approximately `408`, `172`, `457`, `25.9`, `57.9`, `91.8`, `99.9`, and
  `32.8 MiB`;
- TUN RX/TX drops: `0/0`;
- ingress pump high-water: `129/500`, full waits `0`, read errors `0`;
- formal aggregate QUIC loss, congestion, and flow-control blocking deltas:
  all `0`;
- terminal pending, terminal reap, late payload, and flush failure: all `0`;
- the active transfer remained eligible and progressing. Post-iperf
  DrainOnly/Recovery observations belonged to completed sockets with zero
  pending bytes, not a stranded data flow.

Endpoint accounting remained exact. The final snapshot was
`available=61,399B`, `live=0`, `outstanding=0`, and
`289,608,889 - 280,836,884 = 8,772,005B` matched socket-sent bytes. Every
sample obeyed the `61,440B` conservation ceiling; the observed maximum was
`61,406B`.

The sanitized archive is under
`/private/tmp/mini_vpn_reverse_p8_5f9da90_shoes/`. Bundle SHA-256:
`ef3f542a41df46a3be1c22abc15f8e9ac97efac8dfd28c441f0da48641e8c25b`.

## UDP And Live-Streaming Regression

The first UDP attempt used an invalid `1200B` iperf payload. IPv4 plus UDP
adds `28B`, so its `1228B` IP packet exceeded the frozen TUN MTU `1200` and
forced fragmentation. Its result is excluded from acceptance. The negative
artifact is retained at
`/private/tmp/mini_vpn_step4_udp1200_invalid_5f9da90_shoes/` with bundle
SHA-256
`d6def4aafba1c754a3e8f69822afeed4b972178378eb2646cbb335dc7c537d30`.

The corrected test used a `1160B` payload, producing a `1188B` IP packet:

| Direction | Offered | Receiver | iperf loss | mini_vpn/TUN drops | QUIC loss delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| forward | `90.0 Mbit/s` | `83.7 Mbit/s` | `20,474/290,940` (`7%`) | `0 / 0` | `0B` |
| reverse/live-streaming | `90.0 Mbit/s` | `90.0 Mbit/s` | `0/290,950` (`0%`) | `0 / 0` | `0B` |

The required high-rate live-streaming/downlink direction is clean at
`90 Mbit/s`. The forward application loss at the extreme offered rate is
retained as an honest path-capacity observation; it did not coincide with a
mini_vpn UDP drop, TUN drop, or within-window client QUIC loss and did not
justify changing a frozen product constant.

The same fresh TUN also repeated reverse TCP P1 at `191 Mbit/s` receiver,
TUN drops `0/0`, pump `53/500`, and zero full waits/read errors. The suite
completed with exit `0`. Its sanitized bundle is under
`/private/tmp/mini_vpn_step4_udp1160_5f9da90_shoes/`, SHA-256
`d2fa16a3339bef6eb38fffb53e0b72298c75f43791115fb767a67e2878898ea3`.

## Linux Fake-IP DNS And TUN Rearm

Two independent process/TUN lifecycles passed:

1. create `tun0`, route `8.8.8.8/32` through it, query
   `example.com A @8.8.8.8`, receive `198.18.0.2`, observe
   `DNS forge=1/drop=0`, then terminate and verify process, TUN, and route
   removal;
2. create a fresh process and fresh `tun0`, query
   `rust-lang.org A @8.8.8.8`, receive `198.18.0.2`, again observe
   `DNS forge=1/drop=0`, then terminate and verify complete removal.

This proves Linux fake-IP interception for an arbitrary plaintext resolver,
clean stop, and subsequent create/start/rearm. The six-file sanitized archive
is `/private/tmp/mini_vpn_step4_dns_rearm_5f9da90_shoes.tar.gz`, SHA-256
`3d28a8b404a0e128c7857286dd6e17733ed5e9b2db3042b57203c2686a27ca28`.

## Final Local Gates And Review

After VPS acceptance, the exact source passed again:

- mini_vpn default library: `632 passed / 3 ignored`;
- harness library: `643 passed / 3 ignored`;
- concurrency integration: `10 passed / 4 ignored`;
- vendored quinn-proto: `309/309` plus `3/3`;
- vendored Quinn with the explicit local proto patch: `29/29`, three expected
  ignored unit tests, and `1/1` doc test;
- vendored smoltcp enabled-feature suite: `290/290` plus `3/3`, one expected
  ignored doc test;
- all-target harness check, root fmt, runner/probe/control syntax and
  self-tests, and diff checks: PASS.

The standalone Quinn manifest must receive the explicit absolute local
quinn-proto patch. A run without it failed to compile against the unmodified
registry proto; the correct-provenance rerun passed and no source repair was
required.

Final review found no unresolved P0/P1. The ACK barrier cannot invent progress
without a prior nonzero queue, remains subordinate to hard pressure/debt and
terminal no-send, is consumed once on the first eligible clean zero, and is
cleared by the existing rearm lifecycle.

## Cleanup And Decision

No macOS TUN ran. `.27` has no client process, `tun0`, target route, or DNS
route. The transient `.111` Shoes service, restoration service, and timer are
inactive; UDP `8443` has no listener; the transient binary/runtime directory
is removed; and `rmem_max`, `wmem_max`, `rmem_default`, and `wmem_default`
are restored to `212992`.

Task 12 step 4 is complete. The accepted chain is:

`c55737e` EndpointPacingService -> `d934f12` batch relay -> `20a0f8c`
bounded TUN ingress -> `e201403` bounded local TCP receive credit -> `5f9da90`
ACK-completion recovery.

Gate A and Gate B remain accepted. Do not reopen bounded sender, cap64,
GSO-only, D3 self-wake, or frozen-parameter tuning from these results.
