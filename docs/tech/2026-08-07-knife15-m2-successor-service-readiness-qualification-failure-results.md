# Knife15 M2 Successor Service Readiness Qualification Failure Results

Date: 2026-08-07

Status: **ARCHITECTURE DISCRIMINATOR FAILED; FORMAL M2 AND M3 REMAIN BLOCKED**

Client artifact:
`/tmp/mini_vpn_knife15_macos_20260807_091815.tar.gz`

SHA-256:
`7c749fb9a7cbc6f2bafdefaefe622b845eaa722c8959c8d04b8e65ce57ae73ce`

Exact source: `7131de4`.

## Verdict

The exact-source qualification passed the direct controls, start/smoke, every
preflight, the long forward/reverse TCP/reverse UDP phases, Endpoint
conservation, and cleanup. The first short forward then produced one complete
Target receiver-zero interval. Service-normalized admission ran exactly as
designed and selected the better of two admitted busy candidates, but the
selected connection was an authenticated replacement successor that had
never established forward path service. This rejects normalized placement as
sufficient and exposes authentication-only successor installation as the next
architectural seam.

This is not an operator, source, route, physical link, TUN, D16, Endpoint
pacing, pool-size, or frozen-parameter failure. It also is not permission to
tune admission scoring: the selected ordering was correct for its inputs.

## Controls And Failure

The direct discriminator completed 300 seconds at `17.659 Mbit/s` with no
sender or receiver zero interval. The tunnel long forward and reverse TCP
completed, and reverse UDP loss was `1.873567%`, below the frozen `3%` SLI.
The ten-second short forward then reported:

```text
client sent:             10,092,544B
Target received:          4,063,232B
client interval zeros:            3
Target interval zeros:            1
first Target interval:           0B
```

Gateway and Exit controls stayed available, Exit RTT remained about `162ms`
with zero control loss, the interface reported zero errors, and the process,
TUN, route, DNS, and cleanup gates passed.

## Exact Admission And Writer Timeline

The control open selected conn1 generation 2 with `active=0`,
`cwnd=12,000B`, and `RTT=163.515ms`. Its reservation made both data candidates
busy. Service-normalized admission then compared:

```text
conn0: active=2, cwnd=8,400B,  RTT=163.153ms
conn1: active=2, cwnd=12,000B, RTT=163.342ms
```

It correctly selected conn1. The data stream consumed its existing startup
priority turn. Exact D16 writer evidence then progressed from `27,482B` after
`39ms` to `517,435B` by about `4.25s`, and eventually acknowledged
`5,893,598B`. The maximum writer wait was `4,111,333us`; final D16 ownership
was zero. Peer `Stopped(0)` happened only after the timed transfer close and
is not the active-transfer cause.

Endpoint maximum ownership was exactly `61,440B`; final
available/live/outstanding was `61,403/0/0B`. The conservation invariant held:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

## Successor Provenance

The original conn1 generation 1 had previously supplied forward service with
`cwnd=780,994B / RTT=164.107ms`. After its PLPMTUD black-hole counter advanced,
bounded auxiliary replacement authenticated generation 2 and installed it in
about `163ms`. The only immediate application use was a roughly `1,543B`
WeatherKit flow. The successor later carried reverse traffic, but no forward
bulk service that could grow or validate its client-to-Exit send path before
the failed short forward. Its forward admission sample therefore remained at
the initial `12,000B` congestion window.

Authentication proved identity and protocol reachability. It did not prove
that the successor could supply even one complete current congestion-window
turn and receive the corresponding ACKs. Installing it immediately changed
new-open ownership from a service-proven generation to a cold generation.

## Observer Limitation

The intended `.33` observer run
`/tmp/mini_vpn_knife15_exit_target_observer_20260807_064333` reached its
two-hour hard timeout before this Mac qualification began. Its bundle SHA-256
is `1d5816388184577642d37fe0ad69a19e5d3fba7dc502ca35db69916a73253868`
and contains no matching packets. Therefore this artifact does not prove the
exact Exit supply timeline.

The client-side cold-successor lifecycle is exact. The previously paired Exit
artifact (`936e8a16...`) is only a comparator showing that another cold-path
failure expressed as decaying QUIC application supply while Exit-to-Target
remained healthy. The next implementation must be justified by the exact
successor installation contract, not by pretending the missing observer is
present.

## Selected Next Seam

Deepen bounded auxiliary replacement so a freshly authenticated successor
must complete one congestion-controlled, Endpoint-paced, ACK-correlated QUIC
service turn before the existing identity/generation CAS may install it. This
is a readiness contract at the Quinn boundary, not a workload warm-up, Target
probe, pool expansion, retry loop, or static capacity threshold.

Do not rerun or tune unchanged service-normalized admission. One new bounded
qualification is allowed only after local TDD, capacity, regression, review,
and paired-observer gates pass.
