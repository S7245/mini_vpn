# Knife15 M2 ACK-Progress Native Loss Recovery Formal Failure Results

Date: 2026-08-14

Status: **FORMAL M2 FAIL; CURRENT STANDARD-TUIC ESTABLISHED-STREAM
CONTINUITY IS NOT SUFFICIENT; DO NOT REPEAT UNCHANGED; M3 BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260814_014536.tar.gz`

Mac SHA-256:
`2c0026847884602d0f44b359b7081974fd4478413fd14cfbd631b880c3203329`

Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260814_014625.tar.gz`

Exit SHA-256:
`ba37874360aebdc0bb189f45876031ad439a520baed1a999e8bda72c35724501`

Exact source: `cdbfe36ae72f9049e766e4c140476af146e15064`.

## Verdict

The exact-source formal run is a genuine M2 failure, not a runner false
negative, operator error, preflight failure, or Exit-to-Target outage. It
passed baseline, direct discrimination, smoke, all preflights, four complete
mixed cycles, 36 phases, four DNS checks, four real-client checks, and cleanup.
Cycle 5 `tcp-forward` then produced five complete Target receiver-zero
intervals, violating the frozen continuity SLI.

The paired evidence selects loss and native Cubic recovery on the Mac-to-Exit
QUIC leg. The established TUIC stream kept ACK progress, but its owning QUIC
connection contracted from `2,675,530B` to `24,463B` cwnd while packet loss,
congestion events, and PLPMTUD black-hole evidence advanced. Standard TUIC
cannot move this already-established Target TCP byte stream to a successor
QUIC generation. Generation replacement remains correct for future opens but
is not sufficient for this continuity requirement.

Do not rerun formal M2 unchanged and do not tune D16, MTU/PLPMTUD, pool, QUIC
windows, chunk, Cubic, GSO, Endpoint pacing, retry, or deadlines around this
result. M3 remains blocked.

## Completed Envelope

- direct baseline: `32.701/56.515 Mbit/s` forward/reverse receiver rate;
- bounded 300-second direct forward: `16.341 Mbit/s`, zero receiver-zero
  intervals;
- formal workload start: `2026-08-14T01:46:37Z`;
- four complete cycles and 36 complete phases;
- four DNS and four real-client checks;
- maximum completed UDP loss: `2.052430%`, below the frozen `3%` limit;
- failure: cycle 5 `tcp-forward` at `2026-08-14T02:49:52Z`;
- cleanup: PASS, with process dead and owned route/DNS/TUN state restored.

The failed 300-second TCP flow sent `571,342,848B`; the Target received
`562,167,808B`. Its aggregate receiver rate was `14.982 Mbit/s`, but complete
receiver intervals `57..58`, `58..59`, `60..61`, `61..62`, and `111..112`
seconds each received zero bytes. The final partial interval is excluded.
The maximum sender/receiver interval gap was `9,175,040B`, below `16MiB`, so
the decisive failure is continuity rather than the bounded-gap limit.

## Mac-to-Exit Attribution

The failed flow opened as stream 99 on pool connection 1 with exact transport
identity `52698029072`, cwnd `2,675,530B`, RTT about `169ms`, and PLPMTUD
black-hole anchor/current `4/4`. During the flow, connection statistics moved:

```text
cwnd:                 2,675,530B -> 24,463B
lost packets:             5,660 -> 6,680
lost bytes:           2,467,761 -> 3,795,148
congestion events:        1,552 -> 2,029
black holes:                  4 -> 36
ACK frames received:    827,611 -> 999,118
```

The exact writer-pressure observer recorded a longest `9,250ms` episode with
`1,015,590B` acknowledged progress, 34 ACK-progress observations, maximum
pending `9,122ms`, and maximum ACK stall only `499ms`. Therefore the current
Endpoint recovery policy correctly did not rebind: the path was degraded but
not ACK-dead. No mini_vpn `path_changed()`, Endpoint rebind, generation
replacement, successor proof, or successor install occurred. The reverse-gap
ACK counter stayed at 29 and is unrelated to this forward supply failure.

Quinn's per-path pacer remained ahead of the Endpoint cap and no send path
bypassed either module. Endpoint conservation stayed within `61,440B`; the
last sample was `61,414/0/0B` available/live/outstanding. Abandoned datagrams,
socket would-block events, and Endpoint delay events were zero. The logged
about-five-second `max_service_gap` is not a continuously-backlogged service
gap: that metric compares consecutive grants across idle periods.

D16/TUN ownership, local TCP retransmits, process health, routes, DNS, and
physical-interface errors were clean. One three-packet Exit ICMP sample lost
one packet about 35 minutes before the failed flow; every failure-window Exit
and gateway sample lost zero, so it is not direct authority for the exact
receiver zeros.

## Paired Exit Evidence

The exact v2 observer started before formal admission and was automatically
frozen on workload failure. Its archive checksum, gzip/tar structure, and all
internal `SHA256SUMS` entries pass. It captured `25,857,901` packets with zero
kernel drops and left no observer state or nftables ownership after bundling.
Final four-direction counters were all nonzero.

During cycle 5, per-second Target-egress bytes fell with TUIC-ingress bytes;
the Exit could only forward what arrived from the Mac. The exact Exit-to-Target
socket nevertheless retained Target ACK progress:

```text
Target payload received:       562,256,515B
Exit TCP retransmitted:              2,725B
maximum Exit send queue:             30,350B
maximum Target payload supply gap: 996.276ms
```

The near-one-second supply gap and low per-second supply line up with the
Target receiver-zero windows. The Target, `.33 -> .77` TCP path, Exit kernel,
capture, and sing-box process are therefore excluded as the primary cause.

## Code Review And Architecture Stop

Targeted review covered the Quinn Cubic recovery epoch, per-path pacer,
Endpoint reservation/conservation path, ACK-progress recovery policy, TCP
pool admission/replacement, successor proof/install CAS, and established
relay ownership. No unresolved local P0/P1 or deterministic implementation
repair was found.

The available recovery branches have exact limits:

1. Re-authorizing same-path `path_changed()` or Endpoint rebind while ACKs
   progress would reopen the formally rejected connection-local reset branch.
2. Generation replacement can deepen new-open admission but cannot migrate an
   established stream or its remote Target TCP socket.
3. Opening a duplicate TUIC Connect creates a second Target TCP socket; its
   bytes cannot be merged into the first socket's sequence.
4. A resumable, path-diverse relay would require a custom Upstream protocol
   and server-side byte ownership. That contradicts ADR-0004's accepted
   client-only, mature sing-box interoperability direction and is not a local
   repair to the current module.
5. VLESS/REALITY provides a real second Transport adapter for future opens,
   but cannot take ownership of an already-established TUIC stream.

The current modules remain deep for their actual interfaces: Endpoint pacing
owns byte/time conservation, native Quinn owns loss recovery, and TCP pool
replacement owns future-open generation handoff. Expanding any of those
interfaces to claim established-stream migration would be false leverage and
would destroy ownership locality.

## Decision

This one-hour formal attempt is not discarded evidence. It reached the exact
current-architecture stop rule much earlier than the 24-hour duration bound
and proves that a clean short qualification plus locally correct lifecycle
repairs cannot guarantee the frozen one-second continuity SLI on this lossy
HK-to-US TUIC path.

Freeze the reviewed `cdbfe36` production behavior. Do not request another
formal M2 until a separate architecture decision either:

- explicitly authorizes a protocol/server change that can own resumable or
  path-diverse established TCP byte streams; or
- changes the release acceptance contract with product-level evidence rather
  than disguising WAN loss as a code or parameter defect.

Neither decision is implicit in this result. Formal M2 remains failed and M3
remains blocked.
