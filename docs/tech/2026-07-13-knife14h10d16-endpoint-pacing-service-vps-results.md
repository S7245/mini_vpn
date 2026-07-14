# Knife14h10d16 Endpoint Pacing Service VPS Results

Date: 2026-07-13
Source: `c55737e1ef094c2bbc0e4e7e69ada97f98219fbb`
Verdict: **FAIL safety acceptance; throughput and endpoint-service gates PASS**

## Scope And Stop Rule

One fresh, target-only, forward-only P1 ran on the accepted `.27 -> .33 ->
.77` topology with `MINI_VPN_TUIC_PACING_POLICY=endpoint-window-v1`. The run
kept H10d16, TUN MTU `1200`, pool `2`, default QUIC MTU/PLPMTUD and windows,
the `64 KiB` application chunk, Cubic, GSO enabled, Quinn's UDP sender, the
driver `20`-datagram work bound, and D3 self-wake disabled. No bounded UDP
sender, PacerCap64, cap sweep, GSO-only branch, macOS TUN, or parameter tuning
ran.

The formal discriminator required receiver throughput strictly above
`170 Mbit/s`, zero TUN RX/TX drops, and aggregate QUIC lost bytes no greater
than `16 MiB`. Throughput passed and QUIC loss stayed below its ceiling, but
the TUN TX drop delta was `10`; the runner propagated failure and stopped
before any full sweep.

Per the endpoint-service architecture stop rule, this closes pacing as a
sufficient product root. No service constant or frozen product parameter may
be tuned in response.

## Build And Preflight

The reviewed source was deployed without `.git`, environment files,
certificates, keys, build output, or logs to `/home/ubuntu/mini_vpn-c55737e`.
Five critical source/lock hashes matched the local commit before the Linux
release build. The suite therefore reports `source_commit=unknown`; the
deployed source identity is instead anchored by the local commit, matched
file hashes, and the following release artifact:

```text
binary_sha256=115f1654f5740f7b300664108b5287e8351204232e94c3c75c340efc62df983f
suite_sha256=df31bcd93d5521c7457f8e3516a25fdbd8287cecfa116396797cc47d010303db
probe_sha256=9542188666709d76b7728d53be6bfaf8d930cab9d2a3cae19b70060aeddc6f11
```

Preflight passed:

- `.33` sing-box active and UDP `8443` listening;
- `.33` `rmem_max/wmem_max=16777216` and
  `rmem_default/wmem_default=1048576`;
- `.77` iperf3 active and TCP `5201` listening;
- `.27` noninteractive sudo available and no old `mini_vpn` process;
- `.27 -> .33` RTT about `0.588 ms` with zero ping loss;
- direct `.27 -> .77` sender/receiver `332/281 Mbit/s`;
- direct reverse sender/receiver `314/282 Mbit/s`;
- `.33 -> .77` sender/receiver `315/262 Mbit/s`;
- `.33 <- .77` sender/receiver `309/286 Mbit/s`.

The external path therefore had sufficient same-window capacity.

## Exact Profile Attribution

Startup verified:

```text
TUN pool=2, MTU=1200, txqueuelen=500
H10d16 byte-owned egress enabled
QUIC CC=Cubic
QUIC GSO policy=enabled
QUIC UDP send service=quinn
QUIC pacing policy=endpoint-window-v1
D3 self-wake=false
```

The fixed endpoint service reported:

```text
rate=30,720,000B/s
burst=61,440B
control_reserve=10,240B
quantum=20,480B
```

Both pool connections were attached to the same endpoint service. The iperf
control stream used connection `0`; the data stream used connection `1`.
There was no reconnect, connection-ID replacement, migration, socket
`WouldBlock`, or stateless-response drop.

## Forward P1 Result

The 20-second P1 completed every interval:

```text
sender/receiver:          211 / 194 Mbit/s
intervals:                20/20 nonzero
tail average/minimum:     196 / 182 Mbit/s
tail collapse:            0
TUN RX/TX drop delta:     0 / 10
QUIC lost-byte delta:     11,922,924B
QUIC congestion delta:    5,123 events
```

The application sender started at `383 Mbit/s`, then settled mostly between
`182` and `204 Mbit/s`. The receiver stayed above the `170 Mbit/s`
architecture discriminator, and the QUIC formal loss delta remained below
`16 MiB`. Five sampled TUN-drop edges accumulated `4 + 2 + 2 + 1 + 1` drops.

The data connection transmitted `507,838,668B` in `396,609` QUIC datagrams.
Its final formal QUIC counters were `11,982,016B` lost and `5,144` congestion
events; the runner subtracted the inherited P1 start state to obtain the
formal delta above.

## Endpoint Conservation And Cleanup

The final endpoint snapshot was:

```text
available=61,403B
live_reservation=0B
outstanding=0B
records=2
granted=507,904,894B / 396,639 datagrams
refunded=56,829B / 342 events
sent=507,848,065B / 396,639 datagrams
abandoned=0B / 0 datagrams
outstanding_high_water=14,520B
would_block=0
delay_events=22,304
max_delay=41us
stateless_sent/dropped=0/0
```

Thus:

```text
61,403 + 0 + 0 <= 61,440
507,904,894 - 56,829 = 507,848,065
```

There was no reservation, outstanding-byte, or abandonment leak. The final
per-connection snapshots remained attached. The two iperf TCP relays closed
with no terminal pending reaping, close-egress bytes, late remote payload, or
TUN flush failure.

The runner did not collect a packet capture in this invocation, so the result
does not claim an empirical `1ms/10ms` pcap bound. The deterministic service
tests already prove the configured window theorem, and the live conservation
and socket-settlement counters show that the service was reached and remained
closed under the formal envelope.

## Failure Attribution

The pacing architecture achieved its capacity goal and reduced the prior
cap64 loss class from `50,621,275B` to `11,922,924B`, but did not eliminate
local TUN ingress drops. This is decisive because the TUN drops occur before
the encrypted UDP socket and cannot be repaired by changing the endpoint
wire-service constants.

The live local path drained `452,225` TUN TCP packets in `6,565` drain
attempts, exhausted a bounded drain budget `3,577` times, and crossed the
backlog guard `494/494` pause/resume edges. The main loop was not CPU
saturated (`15.1-25.6%` active), local/remote relay queues did not report
pressure, and QUIC flow-control blocking remained zero.

Code reachability shows a more specific amplification seam: every packet
handled inside `drain_ready_tun_rx` calls `process_ready_tun_rx_packet`, which
runs `process_dirty_relay`; the enclosing local-egress cycle then services the
dirty set again. The `452,225` drained packets therefore caused at least that
many relay-service passes even though only `6,561` bounded actor cycles were
needed. The next falsifiable branch is to preserve packet ingestion and D16
ownership while coalescing relay service once per bounded TUN drain batch.

## Artifacts And Cleanup

Sanitized evidence is retained at:

```text
/private/tmp/mini_vpn_endpoint_window_v1_c55737e/
mvpn_knife14_endpoint_c55737e_usclient_suite_20260714_090500.tar.gz
sha256=d993d7850271da530fa5e377d40459b5b45742984b206279d442e95a6a0bebb1
```

The archive contains the suite report, client log, P1 report, active socket
samples, and bounded server evidence. It contains no environment file,
private key, unredacted UUID, or password. Cleanup left zero `mini_vpn`
processes on `.27` and restored the `.77` route to `eth0`.
