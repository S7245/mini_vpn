# Knife15 M2 Successor Service Certificate Qualification Failure Results

Date: 2026-08-10

Status: **QUALIFICATION FAILED; EXACT PAIRED EVIDENCE REJECTS FLIGHT OWNERSHIP AS SUFFICIENT AND SELECTS STALE SUCCESSOR SERVICE READINESS; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac artifact:

```text
9d291f6c0a1c557f238925488ac46984a37b970306ea97491ddb6c7237d18ce8
/tmp/mini_vpn_knife15_macos_20260810_055556.tar.gz
```

Exact source: `80df179cf85b1b4d5e590a69396fdf5b97f84f37`, a pushed
descendant of flight-ownership implementation `c218d8a`.

Paired Exit artifact:

```text
6d5538c95e1dcbff363bbf8577064c09f0a3b54269e6454589f6ddc293cd207f
/tmp/mini_vpn_knife15_exit_target_observer_20260810_054143.tar.gz
```

## Envelope

- direct baseline forward/reverse receiver rates were
  `23.658/61.795 Mbit/s`;
- the bounded 300-second direct discriminator passed at
  `11.822 Mbit/s`, with zero sender/receiver-zero intervals;
- start, smoke, IPv6, full-tunnel, controlled-drain, DNS, real-client, and
  every other qualification preflight passed;
- cycle 1 and cycle 2 long forward/reverse TCP and reverse UDP passed;
- cycle 2 short forward failed one complete Target receiver interval;
- formal M2 was not run; route/DNS/TUN/process cleanup passed.

This is exact qualification evidence, not formal acceptance.

## Exact Client Evidence

The failed data relay used conn1 stable id `51183265808`, generation 2,
stream 7, writer 8. The immediately adjacent control relay used stream 6 on
the same generation. Before the open, admission observed:

```text
conn1 service-turn installed cwnd:       24,800B
conn1 current cwnd before control open:  17,360B
conn1 current RTT:                       172.869ms
conn1 current black holes:               0
conn1 current congestion events:         1
conn0 alternative cwnd:                  12,000B
```

Because forward qualification owns only a PLPMTUD black-hole epoch, conn1 was
still classified `qualified`. Its idle liveness probe passed, then both the
iperf control and data opens selected it.

During the failed ten-second business phase:

```text
local iperf sender bytes:                 6,160,384B
Target application bytes:                1,441,792B
D16 writer progress bytes:                3,148,248B
maximum D16 writer wait:                  3,856,226us
exact writer ACK progress:                89,119 -> 1,480,322B
conn1 final cwnd:                         40,322B
conn1 lost-byte delta:                    9,863B
conn1 congestion-event delta:             6
```

Every writer-pressure episode made ACK progress, so the existing ACK-stall
recovery correctly did not fire. D16 queued/leased/reserved ownership settled
at `0/0/0B`. Endpoint conservation remained at or below `61,440B`, interface
errors remained zero, and cleanup was complete.

The repeated receiver-zero interval proves that exact accepted-offset flight
ownership is present and reachable but is not sufficient when a previously
qualified successor has lost its proved starting service before a later new
open.

## Paired Exit Evidence

The exact business socket was `172.26.0.2:49870 -> 43.130.32.77:5201`.
Across `10.210s`, the capture saw `1,506,514B` of TCP payload. Its largest
positive Exit-to-Target supply gap was `280.203ms`; Target ACKs returned in
about `1..6ms`, sender TCP retransmit growth was zero, and the observer
recorded zero kernel drops.

Approximate supplied bytes by second from phase start were:

```text
42,204 97,989 106,245 131,114 131,135 155,967
164,251 169,743 184,934 197,375 125,557 bytes
```

The Exit forwarded promptly whenever QUIC supplied bytes. Same-window Exit
and gateway probes also had zero loss. This rejects operator timing, the
direct path, Target, Exit-to-Target TCP, capture loss, TUN, D16, Endpoint,
routes, process, and cleanup as the failed interval's owner.

## Selected Mechanism

The successor service turn proved one fresh generation at approximately
`24,800B` cwnd. Later ordinary congestion reduced that same generation to
`17,360B`, but the only persisted admission certificate was the unchanged
black-hole anchor. An idle liveness datagram then proved reachability, not
the transport service state required by the short forward flow.

At about `173ms` RTT, ideal slow-start capacity from the stale starting point
is:

```text
17,360 * (1 + 2 + 4) = 121,520B < 128KiB
```

The service-turn-proved starting point instead gives:

```text
24,800 * (1 + 2 + 4) = 173,600B > 128KiB
```

This matches the first receiver interval discriminator without creating a
configured cwnd threshold. The missing contract is a generation- and
path-owned record of the service floor that the successor actually proved.

## Stop Boundary

Retain the exact flight-ownership implementation as a correct necessary
invariant, but reject it as sufficient. Do not repeat this source or tune D16,
MTU, pool, windows, chunk, Cubic, GSO, Endpoint, self-wake, workload, rate,
burst, or SLI values. Add a transport-native successor service certificate,
invalidate admission when its exact generation/path changes or current cwnd
falls below its own proved floor, and reuse the bounded auxiliary generation
replacement seam before another Mac qualification.
