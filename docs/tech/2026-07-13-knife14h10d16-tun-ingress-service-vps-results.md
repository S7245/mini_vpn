# Knife14h10d16 TUN Ingress Service VPS Results

Date: 2026-07-13
Source: `20a0f8cca6ae0661497173671ea4f426333d6114`
Verdict: **ARCHITECTURE FAIL; capacity/mechanism/QUIC gates PASS, bounded-ingress safety FAIL**

## Scope And Frozen Profile

One isolated target-only, forward-only P1 ran on `.27 -> .33 -> .77` after a
successful profile rehearsal. It preserved H10d16, TUN MTU `1200`, kernel TUN
queue `500`, pool `2`, `1 MiB` TCP socket buffers, default QUIC MTU/PLPMTUD and
windows, `64 KiB` application chunks, Cubic, GSO enabled, Quinn's UDP sender,
EndpointWindowV1, the `20`-datagram driver work bound, D3 actor disabled, and
self-wake disabled.

No FIFO, batch, drain, pacing, MTU, queue, pool, window, chunk, congestion,
GSO, sender, driver, or self-wake parameter changed. No macOS TUN ran.

The discriminator required:

- receiver throughput strictly above `170 Mbit/s` with `20/20` intervals;
- zero TUN RX and TX drops;
- aggregate QUIC lost-byte delta no greater than `16 MiB`;
- pump high water below `500` and zero full waits/read errors;
- exact batch/poll/flush/relay equality and endpoint conservation;
- clean lifecycle and cleanup.

## Source, Build, And Rehearsal

The exact source archive omitted `.git`, environment files, certificates,
keys, logs, and build output. Local and remote SHA-256 matched:

```text
source archive = cb4ed3a43712307be023a167c632579ad80f96ef8d8a3a9635156158a8152473
binary         = 217a231f11944c52a7e3ac762e3f02432f1cdecea9b66ee5e9d11281b82fc388
suite          = a336d5e5b181a08fda5d33d2ab767957ea427e658369ae6f623168156b827d5c
probe          = 79eab40ed884efb225e830ba9a03b1399257a13cbf47e586a9c5be1766597fdd
```

Critical `device.rs`, `client_tun.rs`, and runner hashes also matched the local
commit. Both runner self-tests passed. `PROFILE_REHEARSAL_ONLY=1` verified:

```text
TUN pool=2, MTU=1200, txqueuelen=500
TUN ingress service enabled, capacity=500
H10d16 enabled, quantum=131072
EndpointWindowV1 rate=30,720,000B/s, burst=61,440B
QUIC MTU policy=default, GSO=enabled, sender=quinn
Cubic, fixed QUIC windows, D3/self-wake disabled
two healthy TUIC pool connections and target-only route
```

No iperf ran during rehearsal, and its cleanup restored the target route.

## Preflight

- `.33` RTT averaged about `0.582 ms` with zero ping loss;
- `.27 -> .77` direct receiver/reverse baselines were `278/277 Mbit/s`;
- `.33 -> .77` direct receiver/reverse baselines were `281/285 Mbit/s`;
- `.33` sing-box/TUIC and `.77` iperf3 services were healthy;
- both `.33` and `.77` remained reachable through their expected direct paths.

## Formal P1 Result

The 20-second P1 completed every interval:

```text
iperf sender / receiver:       398 / 194 Mbit/s
receiver intervals:           20/20 nonzero
tail average / minimum:       210.5 / 199 Mbit/s
tail collapse:                no
TUN RX / TX drop delta:       0 / 419
aggregate QUIC loss delta:    11,770,368B
QUIC congestion delta:        5,161 events
flow-control blocked deltas:  all zero
```

Throughput and QUIC loss passed. Zero-drop safety failed, so the runner stopped
before any sweep.

## Ingress Mechanism Attribution

The final aggregate was:

```text
packets = TCP:                    856,043
pump packets / bytes:            856,043 / 1,026,472,586B
TCP batches:                     4,529
dirty-relay passes:              4,529
batch iface polls:               4,529
batch flushes:                   4,529
batch packet high water:         240
avoided relay / iface polls:     851,514 / 851,514
pump high water / capacity:      500 / 500
pump full waits / read errors:   347 / 0
drain budget exhausted:          6,891
drain would-block:               6,751
```

The code reached the intended product path and coalesced all three actor-owned
operations exactly. It still exhausted the bounded userspace FIFO during the
initial burst. The runtime TUN counter recorded seven drop events, with a
maximum single delta of `405`, and finished at `419` drops.

This is not CPU saturation: load-window loop active was only `4.8-17.8%`,
poll `1.6-5.9%`, and relay `0.6-3.1%`. It is a service/scheduling envelope
failure under the real sub-millisecond startup burst. A continuous reader plus
bounded actor batch did not provide the required jitter headroom.

## Endpoint Conservation And Lifecycle

The final endpoint snapshot remained exact:

```text
available=61,403B live=0B outstanding=0B records=2
granted=507,318,166B refunded=60,244B sent=507,257,922B
abandoned=0B would_block=0 outstanding_high_water=14,520B
```

Therefore:

```text
61,403 + 0 + 0 <= 61,440
507,318,166 - 60,244 = 507,257,922
```

There was no pool reconnect, migration, socket-blocked byte leak, flow-control
block, terminal pending reap, pending-at-close byte, or terminal late payload.
Cleanup left no `mini_vpn` process and restored `.77` to `eth0`.

## Decision And Next Reachability Gate

The bounded ingress-pump/batch architecture is closed as the zero-drop repair.
Do not increase the 500-packet FIFO or kernel queue, change the 48/240 drain
bounds, or tune MTU, pacing, pool, QUIC windows, chunk, Cubic, GSO, driver work,
or self-wake.

The next architecture must explain and reproduce the observed combination:

```text
actor CPU headroom present
exact batch service reachable
500-packet userspace FIFO saturated
kernel TUN dropped 419 packets
largest drop edge occurred during the startup microburst
```

Before another implementation, add a deterministic startup-burst replay with
the frozen 500+500 packet envelope and the real actor scheduling seam. The next
design is eligible only if code-level capacity math and that replay prove a
zero-drop path without enlarging capacities or changing frozen parameters.

## Sanitized Artifact

```text
/private/tmp/mini_vpn_tun_ingress_20a0f8c/
mvpn_knife14_ingress_20a0f8c_usclient_suite_20260714_105201.tar.gz
sha256=2234c88024d5e733009056379e23d1615490638b4f18e35e9853502060e07ab7
```

The five-member bundle passed filename and content denylist scans and contains
no environment file, certificate, private key, unredacted UUID/password, or
build output.
