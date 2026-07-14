# Knife14h10d16 Local Uplink Window Service VPS Results

Date: 2026-07-13
Source: `e20140340f7f949fa8bad9e960ce94451d2c0229`
Verdict: **VPS PASS; zero-drop `>170 Mbit/s` successor accepted**

## Scope And Frozen Profile

One isolated target-only, forward-only P1 ran on `.27 -> .33 -> .77` after a
successful profile rehearsal. It preserved H10d16, TUN MTU `1200`, kernel TUN
queue `500`, pump capacity `500`, pool `2`, `1 MiB` physical smoltcp RX/TX
storage, default QUIC MTU/PLPMTUD and windows, `64 KiB` application chunks,
Cubic, GSO enabled, Quinn's UDP sender, EndpointWindowV1, the `20`-datagram
driver work bound, D3 actor disabled, and self-wake disabled.

The only successor seam was the committed H10d16 local TCP receive-credit
limit: physical storage stayed `1,048,576B`, while advertised and accepted
credit was bounded independently at `368,640B`. No FIFO, kernel queue, batch,
drain, pacing, MTU, pool, QUIC-window, chunk, congestion-control, GSO, sender,
driver, or wake parameter changed. No macOS TUN ran.

The discriminator required:

- receiver throughput strictly above `170 Mbit/s` with `20/20` intervals;
- zero TUN RX and TX drops;
- pump high water below `500`, with zero full waits and read errors;
- aggregate QUIC lost-byte delta no greater than `16 MiB`;
- `recv_queue_max <= 368,640B` and the startup policy fingerprint;
- exact endpoint conservation and batch/poll/flush/relay attribution;
- bounded, attributable close-tail state and complete route/process cleanup.

## Source, Build, And Rehearsal

The deployment archive was exported from the exact Git object, then stripped
of tracked development certificates and keys before transfer. It contained no
`.git`, environment file, credential, certificate, private key, log, or build
output. Local and remote SHA-256 matched:

```text
source archive = 6e3366c52f93ed76eab4f9c134d7f8ed2ded68e908df76dc8a26d2890dcd5ed3
binary         = 0abf9a38d810dd49aa11cf1aa2617767df2f1287ad04c3a2e730f4fb181107aa
suite          = a336d5e5b181a08fda5d33d2ab767957ea427e658369ae6f623168156b827d5c
probe          = 79eab40ed884efb225e830ba9a03b1399257a13cbf47e586a9c5be1766597fdd
```

Critical source files also matched their local hashes. The isolated release
build passed. `PROFILE_REHEARSAL_ONLY=1` verified:

```text
TUN pool=2, MTU=1200, txqueuelen=500
TUN ingress service enabled, capacity=500
physical TCP RX/TX storage=1,048,576B
H10d16 receive_window_limit=368,640B
EndpointWindowV1 rate=30,720,000B/s, burst=61,440B
QUIC MTU policy=default, GSO=enabled, sender=quinn
Cubic, fixed QUIC windows, D3/self-wake disabled
two healthy TUIC pool connections and target-only route
```

No iperf ran during rehearsal. It removed the client and restored the target
route before the formal window.

## Preflight

- `.33` RTT averaged about `0.585 ms` with zero ping loss;
- `.27 -> .77` direct receiver/reverse baselines were `280/281 Mbit/s`;
- `.33 -> .77` direct receiver/reverse baselines were `282/282 Mbit/s`;
- `.33` sing-box/TUIC and `.77` iperf3 services were healthy;
- `.33` retained the required `16 MiB` socket-buffer maxima and `1 MiB`
  defaults.

## Formal P1 Result

The 20-second P1 completed every interval:

```text
iperf sender / receiver:       290 / 191 Mbit/s
receiver intervals:           20/20 nonzero
tail average / minimum:       201.333 / 192 Mbit/s
tail collapse:                no
TUN RX / TX drop delta:       0 / 0
aggregate QUIC loss delta:    11,459,701B
QUIC congestion delta:        5,130 events
flow-control blocked deltas:  all zero
```

Receiver throughput, interval stability, zero-drop safety, and the QUIC-loss
ceiling all passed. The suite returned `0` and stopped after the single
declared P1.

## Receive-Credit And Ingress Attribution

The deployed startup log printed
`receive_window_limit=368640B`. The data socket reached, but never exceeded,
that exact receive-queue limit. At the frozen MTU, the capacity proof predicted
`ceil(368,640 / 1,160) = 318` maximum full-payload packets. The real pump high
water was exactly `318/500`:

```text
TCP recv_queue_max:                368,640B
TUN pump packets / bytes:          623,222 / 746,315,792B
pump high water / capacity:        318 / 500
pump full waits / read errors:     0 / 0
TCP batches:                       4,022
dirty-relay passes:                4,022
batch iface polls / flushes:       4,022 / 4,022
batch packet high water:           240
avoided relay / iface polls:       619,200 / 619,200
```

This closes the prior `500/500`, `347`-wait, `419`-drop failure with a
protocol-credit bound rather than more buffering or faster polling. The
continuous reader and bounded actor batch remain active, but are no longer
asked to absorb a sender-admitted `1 MiB` startup window.

## Endpoint Conservation And Lifecycle

The final endpoint snapshot was exact:

```text
available=61,403B live=0B outstanding=0B records=2
granted=500,763,062B refunded=59,480B sent=500,703,582B
abandoned=0B outstanding_high_water=14,520B would_block=0
delay_events=19,195 max_delay=47us stale_wakers=0
```

Therefore:

```text
61,403 + 0 + 0 <= 61,440
500,763,062 - 59,480 = 500,703,582
```

There was no pool reconnect, pacing migration/detach, socket-blocked byte leak,
flow-control block, terminal pending reap, pending-at-close byte, terminal late
payload, permit leak, or TUN flush failure. The iperf data relay's target-close
tail ended as `remote_write_failed` with zero D16 queued/leased/reserved bytes;
its remaining local receive queue was bounded at the advertised `368,640B`
edge rather than being an unbounded or hidden owner. The auxiliary relay closed
cleanly. Final route/process cleanup restored `.77` to `eth0` and left no
`mini_vpn` process.

## Decision

The local uplink window service is accepted as the zero-drop H10d16 successor.
It supplies the previously missing admission bound while preserving physical
storage for lifecycle resilience and without reopening a frozen parameter
branch. The accepted chain is:

```text
EndpointWindowV1 wire service
-> bounded local TCP receive credit
-> continuous 500-packet TUN ingress pump
-> bounded batch/poll/flush/relay service
```

For this frozen single-flow P1, it sustains the `>170 Mbit/s` receiver target,
keeps both bounded ingress layers below capacity, and preserves endpoint and
close-tail accounting. Future work must retain the independent distinction
between EndpointWindowV1 wire bytes and TCP payload credit; their shared
`368,640` numeric value is a selected capacity coupling, not a unit identity.

## Sanitized Artifact

```text
/private/tmp/mini_vpn_local_uplink_window_e201403/
mvpn_knife14_uplink_window_e201403_p1_usclient_suite_20260714_114044.tar.gz
sha256=769dadd36d1b6db8ba0ff4aad3bd7efc16699b5cc01530932e81cdfb018f18d4
```

The five-member bundle passed path-traversal and content denylist scans and
contains no environment file, certificate, private key, unredacted
UUID/password, source tree, or build output.
