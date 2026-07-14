# Knife14h10d16 TUN RX Batch Service VPS Results

Date: 2026-07-13
Source: `d934f124a54caf1a60963c7ff84bf619a73b2262`
Verdict: **FAIL safety acceptance; batch mechanism, throughput, and QUIC-loss gates PASS**

## Scope And Frozen Profile

One fresh target-only, forward-only P1 ran on `.27 -> .33 -> .77` after a
successful profile rehearsal. It kept H10d16, TUN MTU `1200`, kernel TUN queue
`500`, pool `2`, `1 MiB` TCP socket buffers, default QUIC MTU/PLPMTUD and
windows, the `64 KiB` application chunk, Cubic, GSO enabled, Quinn's UDP
sender, EndpointWindowV1, driver work bound `20`, D3 actor disabled, and
self-wake disabled.

No batch/drain constant, pacing constant, MTU, queue, pool, window, chunk,
congestion controller, GSO policy, or sender was changed. No macOS TUN ran.

The discriminator required:

- receiver throughput strictly above `170 Mbit/s`;
- zero TUN RX and TX drops;
- aggregate QUIC lost-byte delta no greater than `16 MiB`;
- `tcp_batches == batch_dirty_relay_passes` with nonzero avoided passes;
- endpoint conservation and clean lifecycle/cleanup.

## Source, Build, And Preflight

The isolated source archive excluded `.git`, environment files, certificates,
keys, logs, and build output. Its SHA-256 was:

```text
16b0514856f4f82b9282a6f7ee73d3fcd1ce6ac2b4f989a008b03f0f5c54b7c6
```

Six critical source/lock/runner hashes matched the local commit. The Linux
release binary and runner hashes were:

```text
binary=9ab8b65d21b2b10fa9dcc22f725be8daab7dc4e6f31953994e8699bbe7d4eb24
suite=df31bcd93d5521c7457f8e3516a25fdbd8287cecfa116396797cc47d010303db
probe=4f171648e7b4ca7af70dc47adbd445b39edb4a819b7e8f0578d600d70c04f8a4
```

Both runner self-tests passed. `PROFILE_REHEARSAL_ONLY=1` verified the exact
safe1200/H10d16/EndpointWindowV1 startup fingerprint, both pool connections,
target-only route, and cleanup before P1.

Preflight remained healthy:

- `.33` sing-box active with UDP `8443` listening;
- `.33` socket maxima `16 MiB`, defaults `1 MiB`;
- `.77` iperf3 active with TCP `5201` listening;
- `.27 -> .33` RTT about `0.585 ms` with zero ping loss;
- `.27 -> .77` direct receiver `287 Mbit/s`, reverse `281 Mbit/s`;
- `.33 -> .77` receiver `285 Mbit/s`, reverse `278 Mbit/s`.

## Formal P1 Result

The 20-second P1 completed all `20/20` nonzero intervals:

```text
iperf sender/receiver:        395 / 194 Mbit/s
target receiver intervals:   mostly 189-190 Mbit/s after startup
TUN RX/TX drop delta:        0 / 38
aggregate QUIC loss delta:   11,717,996B
aggregate congestion delta:  5,084 events
```

Throughput and QUIC loss passed. The zero-drop safety gate failed, and the
runner returned status `1` and stopped before any sweep.

## Batch Mechanism Attribution

The final batch snapshot was:

```text
attempts=8,348
packets=tcp=847,092
tcp_batches=3,712
batch_dirty_relay_passes=3,712
tcp_batch_packets_high_water=240
avoided_dirty_relay_passes=843,380
budget_exhausted=6,738
backlog_pause/resume_edges=653/653
errors=0
```

The implementation therefore reached the product path and removed the
intended per-packet dirty-relay traversal. It did not remove per-packet
`Interface::poll` and TUN flush work, and it did not create an independent
owner that continuously drains the kernel-facing TUN fd.

The loop stayed below CPU saturation but was poll-dominated:

```text
loop active: 20.0-62.6%
poll:        14.1-44.0%
relay:        0.8- 2.8%
```

This rejects dirty-relay traversal alone as the remaining sufficient root.

## Endpoint Conservation And Lifecycle

The final endpoint snapshot stayed conservative and leak-free:

```text
available=61,409B live=0B outstanding=0B records=2
granted=507,549,344B refunded=55,429B sent=507,493,915B
abandoned=0B would_block=0 max_delay=46us
```

Thus `61,409 + 0 + 0 <= 61,440` and
`507,549,344 - 55,429 = 507,493,915`. There was no migration, socket block,
stateless drop, reservation leak, or outstanding-byte leak. Cleanup left no
`mini_vpn client-tun` process and restored the `.77` route to `eth0`.

## Architecture Decision

The batch-only branch is closed as a product repair. Do not tune the existing
48/240 drain bounds, kernel queue, MTU, pacing service, pool, QUIC windows,
chunk, Cubic, GSO, or self-wake in response.

The next falsifiable architecture must own the kernel-facing read cadence and
remove the remaining per-packet poll/flush amplification:

1. a dedicated bounded TUN RX pump continuously reads the fd into a FIFO whose
   capacity is derived from the already frozen kernel queue estimate;
2. the actor classifies and stages a bounded packet batch;
3. one `Interface::poll`, one TUN flush, and one dirty-relay service process
   that TCP batch.

If that architecture cannot keep the local exact gate above `170 Mbit/s` with
zero modeled kernel/ring drops, or if its frozen VPS P1 has any TUN drop, it is
an architecture failure and must not be rescued by capacity tuning.

## Sanitized Artifact

```text
/private/tmp/mini_vpn_tun_batch_d934f12/
mvpn_knife14_batch_d934f12_usclient_suite_20260714_095656.tar.gz
sha256=508a4c36c9ddf0dba26a3f9d11b528067cf8280b90b5f64d67f450d116d92879
```

The archive passed member and content denylist scans and contains no private
key, certificate, environment file, unredacted password, or build output.
