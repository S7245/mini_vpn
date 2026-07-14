# Knife14h10d16 Reverse P8 Failure Results

Date: 2026-07-13
Source: `55792b33be3839a8505f9548696bcf748ef15e70`
Verdict: **ARCHITECTURE FAIL; D16 ACK-completion evidence can be consumed under hard pressure**

## Scope And Frozen Profile

One fresh target-only, reverse-only, eight-flow, 60-second gate ran on
`.27 -> .33 -> .77` after the runner seam and an exact no-iperf rehearsal
passed. It retained H10d16, EndpointWindowV1, TUN MTU `1200`, kernel queue and
pump capacity `500`, the `48/240` ingress-service bounds, pool `2`, physical
smoltcp RX/TX storage `1 MiB`, local receive credit `368,640B`, fixed QUIC
windows, `64 KiB` chunks, Cubic, GSO enabled, Quinn's sender, driver work bound
`20`, and disabled D3/self-wake paths. No parameter was tuned and no macOS TUN
ran.

The deployment archive contained `748` members and matched locally/remotely:

```text
source archive = 7af6e51382071f8e556f8f2b325e012a84cb93d4ef8e3cd74f3d5a3affb0360a
binary         = 0abf9a38d810dd49aa11cf1aa2617767df2f1287ad04c3a2e730f4fb181107aa
suite          = 7d43d61cc88893e43c6dc335fc227dad2ca709e3c23f36aeb1c2c2261ae8db07
probe          = 79eab40ed884efb225e830ba9a03b1399257a13cbf47e586a9c5be1766597fdd
```

Direct `.27/.33 -> .77` preflights remained in the `279-281 Mbit/s` class and
the `.27 -> .33` RTT was about `0.58 ms`. The remote services and required
socket buffers were healthy.

## Formal P8 Failure

All eight iperf data relays plus one control relay opened across the two pool
connections without reconnect or stale-slot replacement. The first aggregate
interval reached only `4.19 Mbit/s`; almost every later interval was zero. The
probe timed out after about `79.98s`:

```text
receiver:              1,004 KiB / 0.103 Mbit/s
sender:                0
parsed flow intervals: 472
shape:                 no_data
probe exit:            124 (timeout)
```

The suite wrapper returned success because reverse-first remains a best-effort
probe, but the iperf result and every P8 throughput/interval requirement failed.
Cleanup still removed the client and restored the target route to `eth0`.

## What Did Not Fail

The kernel/TUN, smoltcp capacity, endpoint pacing, pool, and QUIC loss paths
were not the limiting owner:

```text
TUN RX/TX drops:              0 / 0
pump high/capacity:           177 / 500
pump full waits/read errors:  0 / 0
send_slice zero/errors:       0 / 0
TUN flush failure/defer:      0 / 0
pending current/high:         0 / 27,840B
send capacity:                1,048,576B throughout
QUIC lost bytes/congestion:   0 / 0
pool reconnects:              0
```

Both QUIC connections continued receiving wire data: connection `0` observed
`55,081,161B` across `32,328` stream frames, and connection `1` observed
`59,365,790B` across `32,263` stream frames. Their peer-generated receive
blocking counters ended at `data/stream=2/5` and `1/4`, respectively. This is
consistent with application-side stream consumption stopping, not with a path
or sender-loss collapse.

Endpoint accounting remained exact:

```text
available=61,403B live=0B outstanding=0B
granted=22,143,434B refunded=19,752,136B sent=2,391,298B
abandoned=0B
```

Therefore `22,143,434 - 19,752,136 = 2,391,298`, and the endpoint invariant
remained below the frozen `61,440B` burst.

## D16 Attribution

Only `1,027,859B` reached smoltcp. The eight data handles each accepted roughly
one D16 actor quantum and then stopped:

```text
handle 1: 133,600B    handle 2: 134,760B
handle 3: 133,684B    handle 4: 133,600B
handle 5: 130,120B    handle 6: 120,232B
handle 7: 124,447B    handle 8: 117,412B
```

The final phase aggregate was `Running=0`, `DrainOnly=8`, `Recovery=1`, with
`156` transitions, `25` Recovery resets, and `130,921` DrainOnly cycles. The
TUN backlog guard itself was clean at shutdown after exactly `6/6`
pause/resume edges, and every data socket was later observed with
`send_queue=0`. Thus device pressure had recovered and the per-flow ACK work
had completed, but eight flows stayed read-paused.

The code-level failure is in
`next_d16_egress_phase_for_snapshot`. A backlog edge arms a per-flow barrier
when `send_queue > 0`. The current function clears that boolean as soon as a
later snapshot sees zero, even when `external_hard_pressure` or drop debt still
forces `transition_egress_phase` to return `DrainOnly`. The positive fact
"nonzero at barrier arm, zero now" is then discarded. Once pressure clears,
the queue is already zero and cannot create another positive cycle-local drain,
so the flow has no evidence with which to enter Recovery.

This explains all principal observations simultaneously:

- one approximately `128 KiB` service quantum per data flow;
- bounded/empty D16 and smoltcp queues rather than growth;
- clean TUN and QUIC loss counters;
- peer flow-control block after application reads stop;
- a clean global backlog guard alongside eight permanently DrainOnly flows.

The single-stream local Quinn tracer did not exercise a multi-flow backlog
pause/resume edge and therefore could not reveal this lost-evidence state.

## Decision

Do not retry P8, tune a bound, change Quinn windows, or add a read waker. The
next repair is an architecture-preserving state transition:

1. retain the per-flow ACK barrier while hard pressure, drop debt, or terminal
   no-send still dominates, even if the current queue snapshot is zero;
2. on the first clean zero snapshot, treat the barrier's nonzero-to-zero
   history as strict positive ACK-completion evidence;
3. atomically consume the barrier and enter Recovery, retaining the existing
   `128 KiB` Recovery quantum and four clean-cycle promotion;
4. prove the lost-evidence interleaving and eight-flow recovery locally before
   one fresh frozen P8.

This changes neither the byte-owned queue, actor exclusivity, DrainOnly safety,
endpoint service, TUN service, nor any frozen capacity.

## Sanitized Artifact

```text
/private/tmp/mini_vpn_reverse_p8_55792b3/
mvpn_knife14_reverse_p8_55792b3_p8_usclient_suite_20260714_115912.tar.gz
sha256=f8249b3df14125b1aa320813f1b3a6639be640c068308300014f82eb8f4e1476
```

The five-member bundle passed bounded member/path and content denylist scans.
