# Knife15 M2 ACK-Progress Path Reset Formal Failure Results

Date: 2026-08-12

Status: **FORMAL M2 FAIL; CONNECTION-LOCAL PATH RESET IS REJECTED; M3
REMAINS BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260812_102923.tar.gz`

Mac SHA-256:
`eb1888a177239f6d9ee7e2594f81238b7ffd6bb2cfa107d153104cf286721e04`

Exact source: `fe7b809688b866fdd7f33132c342e2487cddacfc`

Paired Exit observer artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260812_103012.tar.gz`

Exit SHA-256:
`ce2478a9bbb8760eca9e3303f64545b9d726b310b8bf90aeb29e05942e7e0788`

## Result

Formal M2 ran for about five hours and eight minutes. It passed baseline,
direct discrimination, smoke, all preflights, twenty complete cycles and 181
phases. Cycle 22 `tcp-forward` then produced three complete Target receiver
zero intervals at approximately `202..203s`, `205..206s`, and `217..218s`.
Formal M2 therefore remains failed.

The client sent `139,591,680B`; the Target received `129,368,064B`. Client TCP
retransmits were zero. The local Endpoint, D16, TUN, interface, process,
route, DNS, full-tunnel, and cleanup gates all passed. Endpoint ownership
finished at `61,403/0/0B`, its maximum was the frozen `61,440B`, and maximum
UDP loss was `2.764%`, below the frozen `3%` limit.

## Paired Exit Attribution

The exact Exit-to-Target data socket was:

```text
172.26.0.2:36836 -> 43.130.32.77:5201
```

The observer captured `35,179,614` packets with zero capture or kernel drops.
Across the failed flow, the maximum Exit payload-supply gap was only
`320.240ms`. Its TCP send queue was negligible, ACKs remained continuous,
and RTT was about `1..8ms`. Several one-second windows received less than one
`131,072B` iperf application block, which explains the Target receiver zeros.
The Exit-to-Target path did not stop; Mac-to-Exit QUIC supply was insufficient.

## Exact Current-Flow Timeline

The failed business flow belonged to stable connection `48801366032`, logical
generation 1, QUIC stream 564. It was admitted with current `cwnd=158,309B`,
about `181ms` RTT, and PLPMTUD black-hole counter `4`.

ACK progress continued on the exact writer. At about 163 seconds, one
black-hole increment nevertheless authorized the client recovery monitor to
call `Connection::path_changed()` on the unchanged network path:

```text
trigger:                         tcp_path_degraded
black-hole counter:              4 -> 5
cwnd before / after:             24,285B -> 12,000B
RTT before / after:              181ms -> 333ms
current MTU before / after:      1280 / 1280
business writer / stream:        565 / 564
```

The Target receiver zeros followed at about 202, 205, and 217 seconds. At
about 204 seconds the degraded generation was replaced. The reviewed current-
service handoff worked as designed: it published a required `24,285B` floor,
and the successor proved `24,800B` before installation.

The already established stream 564 correctly remained on the draining
predecessor because TUIC cannot migrate or replay an arbitrary TCP byte stream
across QUIC connections. It continued slowly and closed as a completed timed-
transfer tail. The successor protected future flows but could not repair the
current flow whose congestion state the client had reset.

## Decision

This is the explicit rejection discriminator from the connection-local path
recovery architecture: `path_changed()` was applied, all local and paired
external controls stayed healthy, and a Target receiver-zero recurred.

The mechanism is rejected, not tuned. Continuous exact ACK progress is proof
that native QUIC loss recovery is still servicing the current path; a
PLPMTUD black-hole increment plus writer `Pending` is not authority to destroy
that congestion state. The next stage must retire this client-forced path
reset permission. Exact ACK-stall Endpoint rebind, generation replacement for
new flows, native Quinn loss recovery, and the reviewed replacement handoff
remain in scope and unchanged.

