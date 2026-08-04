# Knife15 M2 Qualified-Lane Collapse Failure Results

Date: 2026-08-04

Status: **M2 FAILED — placement hypothesis rejected; local successor required**

## 1. Immutable Evidence

```text
889cdd276c714df20b64a62471a06bea116e1d2d2526d8b1dbe820cb49563ecc
/tmp/mini_vpn_knife15_macos_20260804_095117.tar.gz

/tmp/mini_vpn_knife15_macos_baseline_20260804_093716
/tmp/mini_vpn_knife15_macos_direct_20260804_093843
```

The uploaded checksum matches its `.sha256` file. The bundle binds exact
source `fd6c34f27...`, runner `34e54516...`, and release binary
`8aca5d8...`. The xiaoou Mac used physical `en0 / Ethernet /
192.168.133.1`; every other VPN remained outside the recorded route/DNS
ownership.

The direct prerequisites were valid:

- baseline receiver forward/reverse: `29.369 / 44.838 Mbit/s`, zero complete
  receiver intervals;
- 300-second direct discriminator: `14.675 Mbit/s`, zero sender/receiver
  intervals;
- start/smoke, exact IPv6 `safe_absent`, controlled full tunnel, public Exit,
  fake-IP HTTPS, and cleanup: PASS.

M2 completed cycle-1 long forward TCP, reverse TCP, and reverse UDP. It failed
the first `short-forward-1` at `2026-08-04T10:05:31Z`.

## 2. Decisive TCP Failure

The ten-second short forward result was:

```text
client sender:  10.794 Mbit/s, zero intervals=0
Target receiver: 4.839 Mbit/s, complete zero intervals=1
first receiver interval: 0.000000-1.001056s = 0B
```

This is a complete first interval, not a partial command tail. The following
intervals were positive.

The reviewed busy-epoch implementation ran exactly as designed. The Target
control and data opens were:

```text
conn0 stream69 active_before=12 qualification=qualified black_holes=0/0
conn0 stream70 active_before=14 qualification=qualified black_holes=0/0
qualification_override=true

conn1 qualification=degraded black_holes=0/16
```

Thus both opens isolated the proven degraded conn1 and used the qualified
alternative. This is the exact stop rule in the prior architecture spec: the
placement hypothesis is rejected and the run must not be repeated unchanged.

The data relay wrote `7,882,385B` in `829` progress events, but one
`poll_write` call waited `1,169,717us`. At admission conn0 had only
`5,140B / 175ms` current `cwnd/RTT` service and already held fourteen lease
halves. Across the short phase it remained live and made transport progress:

```text
black_holes:       0 -> 0
lost packets:    334 -> 335
lost bytes:    8,542 -> 9,951
congestion events: 9 -> 10
cwnd:          5,140 -> 198,462B
QUIC UDP TX: 589,601 -> 594,333 packets
```

The final `Stopped(0)` writer error occurred only after the runner rejected
the receiver-zero result and terminated iperf. Its D16 ownership closed at
`queued/leased/reserved=0/0/0`; it is consequence, not cause.

## 3. Rejected Causes

During the exact failure window:

- Exit probes at `10:05:10Z` and `10:05:31Z` were `3/3`, zero loss, about
  `162-165ms`;
- gateway probes were `3/3`, zero loss, about `0.22-0.26ms`;
- physical interface errors remained zero;
- Endpoint conservation remained bounded and ended
  `available/live/outstanding=61,412/0/0B`;
- Endpoint would-block and delay events were zero;
- D16, TUN pump, smoltcp flush, routes, DNS, process resources, and cleanup
  passed.

This rejects operator/source error, a direct-path continuity failure, a
same-window physical outage, dead QUIC connection, PLPMTUD advance on the
selected lane, Endpoint pacing capacity, D16 ownership, TUN drain, and route
or cleanup failure.

The selected mechanism is qualified-lane collapse: isolating conn1 reduced
new-open service to one already-busy persistent generation. Qualification is
valid negative admission evidence, but it cannot manufacture a clean second
lane. A selector-only module therefore cannot satisfy the observed contract.

## 4. Independent UDP Violation

The preceding 180-second reverse UDP phase completed its command but recorded:

```text
loss = 3.391937%
frozen limit = 3.0%
```

Its exact Exit/gateway control rows were lossless, while mini_vpn reported
zero UDP uplink/downlink drops and zero datagram backpressure. The bundle has
no same-path mature-client or exact Exit-to-client datagram control, so this
evidence cannot honestly distinguish WAN/sing-box datagram loss from a TUIC
quality failure. It remains an independent fail-closed discriminator and
does not authorize CC, MTU, payload, pacing, or SLI tuning.

The formal runner currently checks UDP loss only in the final M2 aggregate;
the phase itself was incorrectly recorded `complete`. This is an observer
lifecycle defect: a run could spend many additional hours after it was
already incapable of acceptance. The repair is to enforce the existing
`<=3.0%` predicate immediately after every formal UDP phase, without changing
the threshold or workload.

## 5. Decision

Do not tune or repeat `fd6c34f` unchanged. Replace the selector-only model
with a bounded auxiliary generation-replacement module:

- a proven degraded busy auxiliary generation becomes drain-only;
- an authenticated successor becomes that logical slot's sole new-open
  generation;
- existing streams remain on the predecessor and are never replayed or
  migrated;
- primary conn0, UDP/health ownership, configured pool `2`, and every frozen
  data-plane value remain unchanged;
- only one predecessor may drain, bounding the current pool to two eligible
  generations plus one non-admitting predecessor.

Architecture and implementation plan:

- `docs/tech/2026-08-04-knife15-m2-auxiliary-generation-replacement-architecture-spec.md`
- `docs/tech/2026-08-04-knife15-m2-auxiliary-generation-replacement-implementation-plan.md`

M3 remains blocked until one complete formal M2 and cleanup PASS.
