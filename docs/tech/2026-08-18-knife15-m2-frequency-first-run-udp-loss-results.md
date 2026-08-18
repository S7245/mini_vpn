# Knife15 M2 Frequency First Run UDP-Loss Result

Date: 2026-08-18

Status: **GENUINE TIER-B QUALITY FAILURE; KNIFE15 CLOSED; M3 BLOCKED**

## Outcome

The first valid Tier-B `m2-frequency` run entered the schedule and completed
eleven cycles. Cycle 12 `udp-reverse` then measured:

```text
lost_packets=13,760
sent_packets=447,313
loss_percent=3.07614578606032
limit_percent=3.0
```

The frozen policy makes this a genuine quality failure. It is not a controller,
environment, cleanup, rounding, or final-partial-interval failure. No six-hour
epoch completed, so the ledger receives zero epoch credit; that does not grant
retry authority. The accepted Knife15 stop rule closes same-class standard
single-path TUIC resource and parameter work and opens the path-diverse,
resumable-upstream architecture stage. M3 remains blocked.

## Immutable Transaction

- source: `d5b83045d77b06294ad02b5de87b95e1c863c01f`;
- release SHA-256:
  `5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032`;
- runner SHA-256:
  `bda2b487d9fecdde0a6c34c5d3dc7d708e82e7d8892b754af415904ef328e43b`;
- resource: `tierb-alibaba-usw1-r1`, EIP `47.89.211.4`;
- resource-profile SHA-256:
  `78275a2e29c758a31856ad0b9948a93213bc8946723f7b634416d23b9fbbd581`;
- baseline: `/tmp/mini_vpn_knife15_macos_baseline_20260818_033718`,
  `27.743/46.080 Mbit/s`, zero receiver-zero intervals;
- preserved baseline SHA-256:
  `fc2288b196c182523d9e82a7db7e1ca6d072bb60bef3b61e7c25ad6122e5cfbc`;
- direct: `/tmp/mini_vpn_knife15_macos_direct_20260818_033801`,
  `13.871680 Mbit/s`, PASS, result SHA-256
  `d80cb245e98fdbf5b655bed71d45a90ac2d7e14dc07cc200780407cb49a680a9`;
- resource archive SHA-256:
  `cdd364f069f06c78dcd5ff72899b2f6661c552b27a1bcb49dda4ceb97a0f3c26`,
  PASS;
- Mac bundle:
  `/tmp/mini_vpn_knife15_macos_20260818_034527.tar.gz`, SHA-256
  `c96b342da9262e1408c562a4ab6ab0567d689a087210982ca6bab11282ef952d`;
- Exit observer bundle SHA-256:
  `567409b7a0fc62be55f708c14ebc19fdfd2dd49228991e520b53e1c67419b468`.

Start and smoke passed. The schedule ran for about `2h55m`. The detached
controller sent the requested completion email once, then completed Mac
status/snapshot/stop and Exit observer finalization.

## Cycle Evidence

Complete `udp-reverse` loss percentages were:

| Cycle | Loss |
|---:|---:|
| 1 | 2.261280% |
| 2 | 2.768916% |
| 3 | 2.216966% |
| 4 | 2.069007% |
| 5 | 2.189105% |
| 6 | 2.412917% |
| 7 | 2.015367% |
| 8 | 2.188177% |
| 9 | 2.518147% |
| 10 | 2.207314% |
| 11 | 2.494897% |
| 12 | **3.076146%** |

Cycle 12 was not a uniform fractional miss. Complete one-second loss bursts
included `49.858%`, `44.081%`, and multiple intervals above `41%`. The final
interval was not used to manufacture the verdict.

## Paired Packet Boundary

The paired Exit capture retained `63,797,374` packets with zero kernel capture
drops. Exact cycle-12 sequence accounting gives:

| Boundary | Packets | Difference from Target |
|---|---:|---:|
| Target datagrams observed at Exit | 447,313 | 0 |
| Exit outer TUIC 1,207-byte egress | 436,189 | 11,124 |
| Mac receiver application packets | 433,553 | 13,760 |

Therefore:

- `11,124 / 447,313 = 2.486849%` was lost after the Exit capture observed the
  Target datagram and before a corresponding outer TUIC datagram was observed;
- a further `2,636 / 447,313 = 0.589297%` was lost after Exit outer egress or
  before application delivery;
- the two boundaries sum to the authoritative `3.076146%` result.

The normal mapping is one `1,160B` Target application datagram to one `1,207B`
outer TUIC datagram. The small number of larger control packets cannot hide
the missing application packet count.

The first boundary selects the Exit UDP-socket / sing-box / QUIC-datagram send
handoff under transport backpressure as the dominant failure domain. It does
not prove one exact internal drop site: the observer did not record live
`SO_RXQ_OVFL`, `/proc/net/udp`, or server QUIC send-queue state.

## Code-Level Reachability Review

The exact server dependency chain is sing-box `v1.13.14`, `sing-quic v0.6.1`,
and `quic-go v0.59.0-sing-box-mod.4`. The downlink path is synchronous:

```text
Target UDP socket
  -> sing packet copy
  -> TUIC udpPacketConn.WritePacket
  -> quicConn.SendDatagram
  -> bounded quic-go datagram queue
```

The quic-go send queue is bounded at 32 datagrams and `SendDatagram` waits for
space. At this workload, one active leg carries approximately:

```text
23.04 Mbit/s * 1,207 / 1,160 = 23.98 Mbit/s wire
32 * 1,207B / 23.98 Mbit/s ~= 12.9ms queued service
```

A longer QUIC congestion stall therefore backpressures the synchronous reader
and can leave the kernel UDP receive socket as the next bounded loss boundary.
This is a standard native-datagram limitation, not evidence that a parameter
change inside mini_vpn can repair the established path.

Moving all UDP to per-packet QUIC streams is not an accepted fallback. ADR-0005
already records severe high-rate stream-mode collapse, and reliable streams
would replace loss with head-of-line delay and setup overhead.

## Client Safety And Cleanup

- Endpoint conservation passed, maximum ownership `61,440B`, terminal
  `61,403/0/0B` available/live/outstanding;
- UDP up/down drops and backpressure, Endpoint would-block, and interface
  errors were zero;
- D16 terminal queued/leased/reserved ownership was `0/0/0B`;
- the only `Stopped(0)` remote write was a clean timed TCP-transfer tail;
- IPv6 returned to Automatic and Exit/Target routes returned to physical
  `en0`;
- no mini_vpn, controller, frequency workload, caffeinate, observer, or
  observer nftables ownership remained;
- sing-box was active with zero restarts.

The failure is not attributed to TUN, D16, Endpoint pacing, local interface
backpressure, Target loss, controller lifecycle, or incomplete cleanup.

## Decision

1. Seal this as `quality_failure/udp_loss` for the Tier-B resource and source.
2. Do not rerun the incomplete epoch, relax `3%`, tune frozen values, add
   another equivalent VPS, or switch to all-stream UDP.
3. Close Knife15 without acceptance; keep M3 blocked.
4. Supersede the enhanced-continuity part of ADR-0004 with a server-owned,
   path-diverse, resumable upstream. Retain standard TUIC as a compatibility
   profile.
5. Require deterministic local byte/packet ownership tests and a bounded
   two-path qualification before another long macOS run.
